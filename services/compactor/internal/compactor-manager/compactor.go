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

	"github.com/linkedin/goavro/v2"
	
	// NEW: Specific imports
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
	StorageRoot    string
	SchemaAPI      string
	CatalogClient  catalogv1.CatalogServiceClient // Renamed from MetadataClient
	MasterClient   coordinatorv1.CoordinatorServiceClient   // Needed to commit the final file
}

type Compactor struct {
	config         Config
	dataNodeClientCache map[string]datanodev1.DataNodeServiceClient
	cacheLock      sync.Mutex
}

func NewCompactor(cfg Config) *Compactor {
	return &Compactor{
		config:         cfg,
		dataNodeClientCache: make(map[string]datanodev1.DataNodeServiceClient),
	}
}

// Helper: Connect to a DataNode
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

// Compact runs the full cycle: Fetch Job -> Read Blocks (Stream) -> Write Parquet -> Commit
func (c *Compactor) Compact(ctx context.Context, jobID string, projectID string, projectName string, schemaName string, targetFiles []string) error {
	// 1. Update job status to 'RUNNING'
	// Note: We use the Catalog for job tracking
	fmt.Println("🚀 Starting compaction job:", jobID)
	_, err := c.config.CatalogClient.UpdateJobStatus(ctx, &catalogv1.UpdateJobStatusRequest{
		JobId:  jobID,
		Status: "RUNNING",
	})
	if err != nil {
		return fmt.Errorf("failed to update job status: %w", err)
	}

	// Defer final status update
	var finalErr error
	defer func() {
		status := "COMPLETED"
		if finalErr != nil {
			status = "FAILED"
		}
		c.config.CatalogClient.UpdateJobStatus(context.Background(), &catalogv1.UpdateJobStatusRequest{
			JobId:  jobID,
			Status: status,
		})
	}()

	// 2. Prepare Local Output
	// In a real system, we would stream this back to DFS, but for now we write locally then upload
	compactedFileName := fmt.Sprintf("compacted-%d.parquet", time.Now().Unix())
	compactedFolder := filepath.Join(c.config.StorageRoot, projectID, schemaName, "compacted")
	os.MkdirAll(compactedFolder, 0755)
	localOutPath := filepath.Join(compactedFolder, compactedFileName)

	// 3. Fetch Schemas
	schemas, err := c.FetchSchema(projectName, schemaName)
	if err != nil {
		finalErr = err
		return err
	}

	// 4. Init Parquet Writer
	fw, err := local.NewLocalFileWriter(localOutPath)
	if err != nil {
		finalErr = err
		return err
	}
	defer fw.Close()

	pw, err := writer.NewJSONWriter(schemas.ParquetSchema, fw, 4)
	if err != nil {
		finalErr = err
		return err
	}
	pw.CompressionType = parquet.CompressionCodec_SNAPPY

	// 5. Iterate over files and blocks
	// We need to ask the Master where these files are located
	for _, filePath := range targetFiles {
		metaResp, err := c.config.MasterClient.GetFileMetadata(ctx, &coordinatorv1.GetFileMetadataRequest{
			ProjectId: projectID,
			FilePath:  filePath,
		})
		if err != nil {
			log.Printf("Skipping file %s: %v", filePath, err)
			continue
		}

		// Process each block in the file
		for _, block := range metaResp.Blocks {
			locations, ok := metaResp.Locations[block.BlockId]
			if !ok {
				log.Printf("No location found for block %s", block.BlockId)
				continue
			}

			// Try to read from the first available worker
			// In production, you'd loop through locations if one fails
			workerAddr := locations.Address // This comes from our new Common proto

			dfsClient, err := c.getDfsClient(workerAddr)
			if err != nil {
				log.Printf("Failed to connect to worker %s: %v", workerAddr, err)
				continue
			}

			// 6. STREAMING READ (Key Change)
			data, err := c.streamBlockFromWorker(ctx, dfsClient, block.BlockId)
			if err != nil {
				log.Printf("Failed to stream block %s: %v", block.BlockId, err)
				continue
			}

			// 7. Convert Avro -> Parquet
			if err := c.appendAvroToParquet(data, pw); err != nil {
				log.Printf("Conversion error on block %s: %v", block.BlockId, err)
			}
		}
	}

	if err := pw.WriteStop(); err != nil {
		finalErr = err
		return err
	}

	fw.Close() // Ensure file is flushed to disk

	// 8. Upload the new compacted file to DFS
	// (Simulated here: In reality, you would stream 'localOutPath' to 'dfsClient.PushBlock')
	// For this example, we assume we just register the local file or upload it.
	// Let's assume we upload it and get a file info back.
	
	// ... Upload logic here ...

	// 9. Commit Atomic Swap on Master
	// This tells the Master: "Delete the old small files, replace them with this big one"
	_, err = c.config.MasterClient.CommitCompaction(ctx, &coordinatorv1.CommitCompactionRequest{
		ProjectId:    projectID,
		OldFilePaths: targetFiles,
		NewFile: &coordinatorv1.CommitFileRequest{
			ProjectId:  projectID,
			FilePath:   filepath.Join(schemaName, compactedFileName),
			FileFormat: "parquet",
			Blocks: []*commonv1.BlockInfo{}, 
		},
	})

	if err != nil {
		finalErr = err
		return fmt.Errorf("failed to commit compaction: %w", err)
	}

	log.Printf("✅ Compacted %d files into %s", len(targetFiles), localOutPath)
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
		// Accumulate chunks
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