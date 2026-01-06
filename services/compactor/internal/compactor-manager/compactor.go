package compactormanager

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/linkedin/goavro/v2"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
)

type Config struct {
	StorageRoot string
	SchemaAPI   string // e.g., "http://schema-service:8080"
}

type Compactor struct {
	config Config
}

func NewCompactor(cfg Config) *Compactor {
	return &Compactor{config: cfg}
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

	if resp.StatusCode == http.StatusNotFound {
		return nil, fmt.Errorf("schema not found for project %s, schema %s", project, schemaName)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("api error: status %d", resp.StatusCode)
	}

	var schemaRes FetchSchemaResponse
	if err := json.NewDecoder(resp.Body).Decode(&schemaRes); err != nil {
		return nil, fmt.Errorf("failed to decode schema response: %w", err)
	}

	// Validation: Ensure both schemas are present
	if schemaRes.AvroSchema == "" || schemaRes.ParquetSchema == "" {
		return nil, fmt.Errorf("received incomplete schema definitions from API")
	}

	return &schemaRes, nil
}

func (c *Compactor) Compact(project, schemaName string) error {
	inputPath := filepath.Join(c.config.StorageRoot, project, schemaName)
	compactedFolder := filepath.Join(inputPath, "compacted")
	os.MkdirAll(compactedFolder, 0755)

	schemas, err := c.FetchSchema(project, schemaName)
	if err != nil {
		return err
	}

	outPath := filepath.Join(compactedFolder, fmt.Sprintf("compacted-%d.parquet", time.Now().Unix()))
	fw, err := local.NewLocalFileWriter(outPath)
	if err != nil {
		return err
	}
	defer fw.Close()

	// NewJSONWriter uses the Parquet Schema JSON string provided by your API
	pw, err := writer.NewJSONWriter(schemas.ParquetSchema, fw, 4)
	if err != nil {
		return fmt.Errorf("failed to init parquet writer: %w", err)
	}
	pw.CompressionType = parquet.CompressionCodec_SNAPPY

	// 3. Process Avro files one by one (Streaming Input)
	entries, _ := os.ReadDir(inputPath)
	var processedFiles []string

	for _, e := range entries {
		if e.IsDir() || filepath.Ext(e.Name()) != ".avro" {
			continue
		}

		fullPath := filepath.Join(inputPath, e.Name())
		if err := c.streamAvroToParquet(fullPath, pw); err != nil {
			log.Printf("Error processing file %s: %v", e.Name(), err)
			continue
		}
		processedFiles = append(processedFiles, fullPath)
	}

	// 4. Finalize Parquet File
	if err := pw.WriteStop(); err != nil {
		return fmt.Errorf("failed to stop parquet writer: %w", err)
	}

	// 5. Cleanup source files only after successful compaction
	for _, path := range processedFiles {
		os.Remove(path)
	}

	log.Printf("✅ Compacted %d files into %s", len(processedFiles), outPath)
	return nil
}

// streamAvroToParquet reads records from a single file and pushes them to the shared writer
func (c *Compactor) streamAvroToParquet(filePath string, pw *writer.JSONWriter) error {
	f, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer f.Close()

	ocfr, err := goavro.NewOCFReader(f)
	if err != nil {
		return err
	}

	for ocfr.Scan() {
		datum, err := ocfr.Read()
		if err != nil {
			return err
		}

		// Convert map to JSON string for the Parquet JSONWriter
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
