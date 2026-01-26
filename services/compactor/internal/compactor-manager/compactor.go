package compactormanager

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"time"

	"github.com/linkedin/goavro/v2"

	"github.com/razvanmarinn/datalake/pkg/dfs-client"
	catalogv1 "github.com/razvanmarinn/datalake/protobuf/gen/go/catalog/v1"
	commonv1 "github.com/razvanmarinn/datalake/protobuf/gen/go/common/v1"
	coordinatorv1 "github.com/razvanmarinn/datalake/protobuf/gen/go/coordinator/v1"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
)

type Config struct {
	StorageRoot   string
	SchemaAPI     string
	CatalogClient catalogv1.CatalogServiceClient
	MasterClient  coordinatorv1.CoordinatorServiceClient
	DFSClient     dfs.Client
}

type Compactor struct {
	config Config
}

func NewCompactor(cfg Config) *Compactor {
	return &Compactor{
		config: cfg,
	}
}

type FetchSchemaResponse struct {
	AvroSchema    string `json:"avro_schema"`
	ParquetSchema string `json:"parquet_schema"`
}

func (c *Compactor) FetchSchema(project, schemaName string) (*FetchSchemaResponse, error) {
	u, err := url.Parse(c.config.SchemaAPI)
	if err != nil {
		return nil, err
	}

	u = u.JoinPath(project, "schema", schemaName)
	resp, err := http.Get(u.String())
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status %d", resp.StatusCode)
	}

	var schemaRes FetchSchemaResponse
	if err := json.NewDecoder(resp.Body).Decode(&schemaRes); err != nil {
		return nil, err
	}
	return &schemaRes, nil
}

func (c *Compactor) Compact(ctx context.Context, jobID string, projectID string, projectName string, schemaName string, targetFiles []string, targetPaths []string, ownerId string) error {
	_, err := c.config.CatalogClient.UpdateJobStatus(ctx, &catalogv1.UpdateJobStatusRequest{
		JobId:  jobID,
		Status: "RUNNING",
	})
	if err != nil {
		return err
	}

	compactedFileName := fmt.Sprintf("compacted-%d.parquet", time.Now().Unix())
	compactedFolder := filepath.Join(c.config.StorageRoot, projectID, schemaName, "compacted")
	if err := os.MkdirAll(compactedFolder, 0755); err != nil {
		return err
	}
	localOutPath := filepath.Join(compactedFolder, compactedFileName)
	defer os.Remove(localOutPath)

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
			req.ResultFilePath = filepath.Join(schemaName, "compacted", compactedFileName)
		}
		_, _ = c.config.CatalogClient.UpdateJobStatus(context.Background(), req)
	}()

	schemas, err := c.FetchSchema(projectName, schemaName)
	if err != nil {
		finalErr = err
		return err
	}

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
		f, err := c.config.DFSClient.Open(ctx, filePath)
		if err != nil {
			continue
		}

		if err := c.appendAvroToParquet(f, pw); err != nil {
			f.Close()
			continue
		}
		f.Close()
		filesProcessed++
	}

	if filesProcessed == 0 {
		pw.WriteStop()
		fw.Close()
		finalErr = fmt.Errorf("no files processed")
		return finalErr
	}

	if err := pw.WriteStop(); err != nil {
		fw.Close()
		finalErr = err
		return err
	}
	fw.Close()

	fullCompactedPath := filepath.Join(projectID, schemaName, "compacted", compactedFileName)
	df, err := c.config.DFSClient.Create(ctx, fullCompactedPath,
		dfs.WithProjectID(projectID),
		dfs.WithFormat("parquet"),
		dfs.WithOwnerID(ownerId),
	)
	if err != nil {
		finalErr = err
		return err
	}

	lf, err := os.Open(localOutPath)
	if err != nil {
		df.Close()
		finalErr = err
		return err
	}

	if _, err := io.Copy(df, lf); err != nil {
		lf.Close()
		df.Close()
		finalErr = err
		return err
	}
	lf.Close()

	if err := df.Close(); err != nil {
		finalErr = err
		return err
	}

	stat, err := df.Stat()
	if err != nil {
		finalErr = err
		return err
	}

	var protoBlocks []*commonv1.BlockInfo
	for _, block := range stat.Blocks {
		protoBlocks = append(protoBlocks, &commonv1.BlockInfo{
			BlockId: block.BlockId,
			Size:    block.Size,
		})

		_, err = c.config.CatalogClient.RegisterDataFile(ctx, &catalogv1.RegisterDataFileRequest{
			ProjectId:  projectID,
			SchemaName: schemaName,
			BlockId:    block.BlockId,
			WorkerId:   block.WorkerId,
			FilePath:   fullCompactedPath,
			FileSize:   block.Size,
			FileFormat: "parquet",
		})
		if err != nil {
			finalErr = err
			return err
		}
	}

	_, err = c.config.MasterClient.CommitCompaction(ctx, &coordinatorv1.CommitCompactionRequest{
		ProjectId:    projectID,
		OldFilePaths: targetPaths,
		NewFile: &coordinatorv1.CommitFileRequest{
			ProjectId:  projectID,
			FilePath:   fullCompactedPath,
			FileFormat: "parquet",
			OwnerId:    ownerId,
			Blocks:     protoBlocks,
		},
	})
	if err != nil {
		finalErr = err
		return err
	}

	return nil
}

func (c *Compactor) appendAvroToParquet(r io.Reader, pw *writer.JSONWriter) error {
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
