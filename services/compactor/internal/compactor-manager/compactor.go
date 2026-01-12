package compactormanager

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/linkedin/goavro/v2"

	// Proto imports
	catalogv1 "github.com/razvanmarinn/datalake/protobuf/gen/go/catalog/v1"
	commonv1 "github.com/razvanmarinn/datalake/protobuf/gen/go/common/v1"
	coordinatorv1 "github.com/razvanmarinn/datalake/protobuf/gen/go/coordinator/v1"
	datanodev1 "github.com/razvanmarinn/datalake/protobuf/gen/go/datanode/v1"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Config struct {
	StorageRoot   string
	SchemaAPI     string
	CatalogClient catalogv1.CatalogServiceClient
	MasterClient  coordinatorv1.CoordinatorServiceClient
}

type Compactor struct {
	config              Config
	dataNodeClientCache map[string]datanodev1.DataNodeServiceClient
	cacheLock           sync.Mutex
	// We keep track of active addresses to pick one for upload later
	activeDataNodes []string
}

func NewCompactor(cfg Config) *Compactor {
	return &Compactor{
		config:              cfg,
		dataNodeClientCache: make(map[string]datanodev1.DataNodeServiceClient),
		activeDataNodes:     make([]string, 0),
	}
}

// Helper: Connect to a DataNode and cache the connection
func (c *Compactor) getDfsClient(addr string) (datanodev1.DataNodeServiceClient, error) {
	c.cacheLock.Lock()
	defer c.cacheLock.Unlock()

	if client, ok := c.dataNodeClientCache[addr]; ok {
		return client, nil
	}

	// addr should be "IP:Port"
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to DFS worker at %s: %w", addr, err)
	}

	client := datanodev1.NewDataNodeServiceClient(conn)
	c.dataNodeClientCache[addr] = client
	c.activeDataNodes = append(c.activeDataNodes, addr) // Track this node as available
	return client, nil
}

type FetchSchemaResponse struct {
	AvroSchema    string `json:"avro_schema"`
	ParquetSchema string `json:"parquet_schema"`
}

func (c *Compactor) FetchSchema(project, schemaName string) (*FetchSchemaResponse, error) {
	url := fmt.Sprintf("%s/%s/schema/%s", c.config.SchemaAPI, project, schemaName)
	resp, err := http.Get(url)
	if err != nil {
		return nil, fmt.Errorf("failed to call schema api: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("schema api error: status %d", resp.StatusCode)
	}

	var schemaRes FetchSchemaResponse
	if err := json.NewDecoder(resp.Body).Decode(&schemaRes); err != nil {
		return nil, fmt.Errorf("failed to decode schema response: %w", err)
	}
	return &schemaRes, nil
}
func (c *Compactor) Compact(ctx context.Context, jobID string, projectID string, projectName string, schemaName string, targetFiles []string, targetPaths []string) error {
	fmt.Println("🚀 Starting compaction job:", jobID)
	_, err := c.config.CatalogClient.UpdateJobStatus(ctx, &catalogv1.UpdateJobStatusRequest{
		JobId:  jobID,
		Status: "RUNNING",
	})
	if err != nil {
		return fmt.Errorf("failed to update job status: %w", err)
	}

	compactedFileName := fmt.Sprintf("compacted-%d.parquet", time.Now().Unix())
	compactedFolder := filepath.Join(c.config.StorageRoot, projectID, schemaName, "compacted")
	if err := os.MkdirAll(compactedFolder, 0755); err != nil {
		return fmt.Errorf("failed to create local temp dir: %w", err)
	}
	localOutPath := filepath.Join(compactedFolder, compactedFileName)

	// Define final status logic
	var finalErr error
	defer func() {
		status := "COMPLETED"
		if finalErr != nil {
			status = "FAILED"
		}

		req := &catalogv1.UpdateJobStatusRequest{
			JobId:  jobID,
			Status: status,
		}
		if status == "COMPLETED" {
			req.ResultFilePath = filepath.Join(schemaName, compactedFileName)
		}

		c.config.CatalogClient.UpdateJobStatus(context.Background(), req)
	}()

	// Fetch Schema
	schemas, err := c.FetchSchema(projectName, schemaName)
	if err != nil {
		finalErr = err
		return err
	}

	// Init Parquet Writer
	fw, err := local.NewLocalFileWriter(localOutPath)
	if err != nil {
		finalErr = err
		return err
	}
	pw, err := writer.NewJSONWriter(schemas.ParquetSchema, fw, 4)
	if err != nil {
		fw.Close()
		finalErr = err
		return err
	}
	pw.CompressionType = parquet.CompressionCodec_SNAPPY

	filesProcessed := 0
	for _, filePath := range targetPaths {
		log.Printf("Processing file: %s", filePath)

		metaResp, err := c.config.MasterClient.GetFileMetadata(ctx, &coordinatorv1.GetFileMetadataRequest{
			ProjectId: projectID,
			FilePath:  filePath,
		})
		if err != nil {
			log.Printf("Skipping file %s due to metadata error: %v", filePath, err)
			continue
		}

		if len(metaResp.Blocks) > 0 {
			filesProcessed++
		}

		for _, block := range metaResp.Blocks {
			locations, ok := metaResp.Locations[block.BlockId]
			if !ok || locations == nil {
				log.Printf("No location found for block %s", block.BlockId)
				continue
			}

			workerAddr := locations.Address
			dfsClient, err := c.getDfsClient(workerAddr)
			if err != nil {
				log.Printf("Failed to connect to worker %s: %v", workerAddr, err)
				continue
			}

			data, err := c.streamBlockFromWorker(ctx, dfsClient, block.BlockId)
			if err != nil {
				log.Printf("Failed to stream block %s: %v", block.BlockId, err)
				continue
			}

			if err := c.appendAvroToParquet(data, pw); err != nil {
				log.Printf("Conversion error on block %s: %v", block.BlockId, err)
			}
		}
	}

	if filesProcessed == 0 {
		fw.Close()
		os.Remove(localOutPath)
		finalErr = fmt.Errorf("compaction aborted: no input files could be read")
		return finalErr
	}

	if err := pw.WriteStop(); err != nil {
		fw.Close()
		finalErr = err
		return err
	}
	fw.Close()

	// Upload
	fileInfo, err := os.Stat(localOutPath)
	if err != nil {
		finalErr = err
		return err
	}

	fmt.Printf("Uploading compacted file (%d bytes)...\n", fileInfo.Size())
	newBlocks, err := c.uploadCompactedFile(ctx, localOutPath)
	if err != nil {
		finalErr = err
		return fmt.Errorf("failed to upload compacted file: %w", err)
	}

	// --- NEW: Register the new file in Catalog (Postgres) ---
	fullCompactedPath := filepath.Join(schemaName, compactedFileName)

	// FIX: Use a valid UUID string
	compactorWorkerUUID := uuid.New().String()

	for _, block := range newBlocks {
		_, err := c.config.CatalogClient.RegisterDataFile(ctx, &catalogv1.RegisterDataFileRequest{
			ProjectId:  projectID,
			SchemaName: schemaName,
			BlockId:    block.BlockId,
			WorkerId:   compactorWorkerUUID, // <--- FIXED HERE
			FilePath:   fullCompactedPath,
			FileSize:   block.Size,
			FileFormat: "parquet",
		})
		if err != nil {
			finalErr = err
			return fmt.Errorf("failed to register compacted file in catalog: %w", err)
		}
	}

	// Commit Atomic Swap
	_, err = c.config.MasterClient.CommitCompaction(ctx, &coordinatorv1.CommitCompactionRequest{
		ProjectId:    projectID,
		OldFilePaths: targetPaths,
		NewFile: &coordinatorv1.CommitFileRequest{
			ProjectId:  projectID,
			FilePath:   fullCompactedPath,
			FileFormat: "parquet",
			OwnerId:    "test-id",
			Blocks:     newBlocks,
		},
	})

	if err != nil {
		finalErr = err
		return fmt.Errorf("failed to commit compaction: %w", err)
	}

	os.Remove(localOutPath)
	log.Printf("✅ Compacted %d files into %s", filesProcessed, compactedFileName)
	return nil
}

func (c *Compactor) streamBlockFromWorker(ctx context.Context, client datanodev1.DataNodeServiceClient, blockID string) ([]byte, error) {
	stream, err := client.FetchBlock(ctx, &datanodev1.FetchBlockRequest{BlockId: blockID})
	if err != nil {
		return nil, err
	}

	var dataBuffer bytes.Buffer
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		dataBuffer.Write(resp.Chunk)
	}
	return dataBuffer.Bytes(), nil
}

func (c *Compactor) appendAvroToParquet(data []byte, pw *writer.JSONWriter) error {
	r := bytes.NewReader(data)
	ocfr, err := goavro.NewOCFReader(r)
	if err != nil {
		return err
	}

	for ocfr.Scan() {
		datum, err := ocfr.Read()
		if err != nil {
			return err
		}

		jsonData, err := json.Marshal(datum)
		if err != nil {
			return err
		}

		if err := pw.Write(string(jsonData)); err != nil {
			return err
		}
	}
	return nil
}

func (c *Compactor) uploadCompactedFile(ctx context.Context, localPath string) ([]*commonv1.BlockInfo, error) {
	file, err := os.Open(localPath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	// Pick a DataNode to upload to.
	c.cacheLock.Lock()
	var client datanodev1.DataNodeServiceClient
	if len(c.activeDataNodes) > 0 {
		client = c.dataNodeClientCache[c.activeDataNodes[0]]
	}
	c.cacheLock.Unlock()

	if client == nil {
		return nil, fmt.Errorf("no active data nodes available for upload")
	}

	const blockSize = 128 * 1024 * 1024 // 128MB Blocks
	const streamChunkSize = 64 * 1024   // 64KB Network Chunks

	buffer := make([]byte, blockSize)
	var blocks []*commonv1.BlockInfo

	for {
		bytesRead, err := file.Read(buffer)
		if err != nil && err != io.EOF {
			return nil, err
		}
		if bytesRead == 0 {
			break
		}

		newBlockID := uuid.New().String()

		stream, err := client.PushBlock(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to create upload stream: %w", err)
		}

		metaReq := &datanodev1.PushBlockRequest{
			Data: &datanodev1.PushBlockRequest_Metadata{
				Metadata: &datanodev1.BlockMetadata{
					BlockId:   newBlockID,
					TotalSize: int64(bytesRead),
				},
			},
		}
		if err := stream.Send(metaReq); err != nil {
			return nil, fmt.Errorf("failed to send metadata for block %s: %w", newBlockID, err)
		}

		currentBlockData := buffer[:bytesRead]
		for i := 0; i < len(currentBlockData); i += streamChunkSize {
			end := i + streamChunkSize
			if end > len(currentBlockData) {
				end = len(currentBlockData)
			}

			chunkReq := &datanodev1.PushBlockRequest{
				Data: &datanodev1.PushBlockRequest_Chunk{
					Chunk: currentBlockData[i:end],
				},
			}

			if err := stream.Send(chunkReq); err != nil {
				return nil, fmt.Errorf("failed to send chunk for block %s: %w", newBlockID, err)
			}
		}

		resp, err := stream.CloseAndRecv()
		if err != nil {
			return nil, fmt.Errorf("datanode stream error for block %s: %w", newBlockID, err)
		}
		if !resp.Success {
			return nil, fmt.Errorf("datanode rejected block %s: %s", newBlockID, resp.Message)
		}

		blocks = append(blocks, &commonv1.BlockInfo{
			BlockId: newBlockID,
			Size:    int64(bytesRead),
		})
	}

	return blocks, nil
}
