package compactormanager

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/linkedin/goavro/v2"
	pb "github.com/razvanmarinn/datalake/protobuf"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Config struct {
	StorageRoot    string
	SchemaAPI      string
	MetadataClient pb.MetadataServiceClient
}

type Compactor struct {
	config         Config
	dfsClientCache map[string]pb.DfsServiceClient
	cacheLock      sync.Mutex
}

func NewCompactor(cfg Config) *Compactor {
	return &Compactor{
		config:         cfg,
		dfsClientCache: make(map[string]pb.DfsServiceClient),
	}
}

// DfsClientCache management
func (c *Compactor) getDfsClient(addr string) (pb.DfsServiceClient, error) {
	c.cacheLock.Lock()
	defer c.cacheLock.Unlock()

	if client, ok := c.dfsClientCache[addr]; ok {
		return client, nil
	}

	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to DFS worker at %s: %w", addr, err)
	}

	client := pb.NewDfsServiceClient(conn)
	c.dfsClientCache[addr] = client
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
		return nil, fmt.Errorf("schema api error: status %d for project %s, schema %s", resp.StatusCode, project, schemaName)
	}

	var schemaRes FetchSchemaResponse
	if err := json.NewDecoder(resp.Body).Decode(&schemaRes); err != nil {
		return nil, fmt.Errorf("failed to decode schema response: %w", err)
	}
	if schemaRes.AvroSchema == "" || schemaRes.ParquetSchema == "" {
		return nil, fmt.Errorf("received incomplete schema definitions")
	}
	return &schemaRes, nil
}

func (c *Compactor) Compact(ctx context.Context, job *pb.CompactionJob) (err error) {
	// 1. Update job status to 'in_progress'
	_, err = c.config.MetadataClient.UpdateCompactionJobStatus(ctx, &pb.UpdateCompactionJobStatusRequest{
		JobId:  job.Id,
		Status: "in_progress",
	})
	if err != nil {
		return fmt.Errorf("failed to update job status to in_progress: %w", err)
	}

	// Defer a function to handle job completion/failure
	defer func() {
		status := "completed"
		if err != nil {
			status = "failed"
		}
		_, updateErr := c.config.MetadataClient.UpdateCompactionJobStatus(context.Background(), &pb.UpdateCompactionJobStatusRequest{
			JobId:  job.Id,
			Status: status,
		})
		if updateErr != nil {
			log.Printf("CRITICAL: Failed to update final job status for job %s: %v", job.Id, updateErr)
		}
	}()

	// 2. Define output path
	compactedFolder := filepath.Join(c.config.StorageRoot, job.ProjectId, job.SchemaName, "compacted")
	os.MkdirAll(compactedFolder, 0755)
	outPath := filepath.Join(compactedFolder, fmt.Sprintf("compacted-%d.parquet", time.Now().Unix()))

	// 3. Fetch schemas
	schemas, err := c.FetchSchema(job.ProjectId, job.SchemaName)
	if err != nil {
		return fmt.Errorf("failed to fetch schemas: %w", err)
	}

	// 4. Setup Parquet writer
	fw, err := local.NewLocalFileWriter(outPath)
	if err != nil {
		return fmt.Errorf("failed to create local file writer: %w", err)
	}
	defer fw.Close()

	pw, err := writer.NewJSONWriter(schemas.ParquetSchema, fw, 4)
	if err != nil {
		return fmt.Errorf("failed to init parquet writer: %w", err)
	}
	pw.CompressionType = parquet.CompressionCodec_SNAPPY

	// 5. Get block locations
	locResp, err := c.config.MetadataClient.GetBlockLocations(ctx, &pb.GetBlockLocationsRequest{BlockIds: job.TargetBlockIds})
	if err != nil {
		return fmt.Errorf("failed to get block locations: %w", err)
	}

	// 6. Process blocks
	for _, loc := range locResp.Locations {
		dfsClient, err := c.getDfsClient(loc.WorkerId) // Assuming WorkerId is the address
		if err != nil {
			return fmt.Errorf("failed to get DFS client for worker %s: %w", loc.WorkerId, err)
		}

		blockResp, err := dfsClient.GetBlock(ctx, &pb.GetBlockRequest{BlockId: loc.BlockId})
		if err != nil {
			log.Printf("Warning: failed to get block %s from worker %s: %v", loc.BlockId, loc.WorkerId, err)
			continue
		}

		if err := c.streamAvroToParquet(blockResp.Data, pw); err != nil {
			log.Printf("Error processing block %s: %v", loc.BlockId, err)
			continue
		}
	}

	// 7. Finalize Parquet File
	if err := pw.WriteStop(); err != nil {
		return fmt.Errorf("failed to stop parquet writer: %w", err)
	}

	log.Printf("✅ Compacted %d blocks into %s", len(locResp.Locations), outPath)
	return nil
}

// streamAvroToParquet reads records from a byte slice and pushes them to the shared writer
func (c *Compactor) streamAvroToParquet(data []byte, pw *writer.JSONWriter) error {
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
