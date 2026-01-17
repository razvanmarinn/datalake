package handlers

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"path/filepath"
	"strings"
	"sync"

	"github.com/gin-gonic/gin"
	"github.com/linkedin/goavro/v2"
	"github.com/razvanmarinn/query_service/internal/grpc"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/source"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
)

type MemoryFile struct {
	*bytes.Reader
}

func (m *MemoryFile) Open(name string) (source.ParquetFile, error) { return m, nil }
func (m *MemoryFile) Create(name string) (source.ParquetFile, error) {
	return nil, fmt.Errorf("read only")
}
func (m *MemoryFile) Close() error { return nil }
func (m *MemoryFile) Write(p []byte) (n int, err error) {
	return 0, fmt.Errorf("read only: write not supported")
}

type QueryHandler struct {
	logger       *zap.Logger
	MasterClient *grpc.MasterClient

	dataNodeClientsMu sync.Mutex
	dataNodeClients   map[string]*grpc.DataNodeClient

	QueryCounter metric.Int64Counter
}

func NewQueryHandler(logger *zap.Logger, masterClient *grpc.MasterClient, queryCounter metric.Int64Counter) *QueryHandler {
	return &QueryHandler{
		logger:          logger,
		MasterClient:    masterClient,
		dataNodeClients: make(map[string]*grpc.DataNodeClient),
		QueryCounter:    queryCounter,
	}
}

func (h *QueryHandler) getDataNodeClient(address string) (*grpc.DataNodeClient, error) {
	h.dataNodeClientsMu.Lock()
	defer h.dataNodeClientsMu.Unlock()

	if client, ok := h.dataNodeClients[address]; ok {
		return client, nil
	}

	h.logger.Info("Creating new connection to DataNode", zap.String("address", address))
	client, err := grpc.NewDataNodeClient(address)
	if err != nil {
		return nil, err
	}

	h.dataNodeClients[address] = client
	return client, nil
}

func (h *QueryHandler) GetFileList(c *gin.Context) {
	projectID := c.Param("project")
	if projectID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "missing project parameter"})
		return
	}

	prefix := c.Query("prefix")

	h.logger.Info("Attempting ListFiles RPC", zap.String("ProjectID", projectID))

	resp, err := h.MasterClient.ListFiles(c.Request.Context(), projectID, prefix)
	if err != nil {
		h.logger.Error("MasterClient RPC Failed", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Internal master service error: " + err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"files": resp.FilePaths})
}

func (h *QueryHandler) GetData(c *gin.Context) {
	h.QueryCounter.Add(c.Request.Context(), 1)

	projectID := c.Param("project")
	fileName := c.Query("file_name")

	if projectID == "" || fileName == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "project and file_name are required"})
		return
	}

	h.logger.Info("Received request for file", zap.String("project", projectID), zap.String("file", fileName))

	metadata, err := h.MasterClient.GetFileMetadata(c.Request.Context(), projectID, fileName)
	if err != nil {
		h.logger.Error("Failed to get metadata", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	var fileBuffer bytes.Buffer

	for _, blockInfo := range metadata.Blocks {
		blockID := blockInfo.BlockId

		location, ok := metadata.Locations[blockID]
		if !ok {
			h.logger.Error("No location found for block", zap.String("block_id", blockID))
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Incomplete file metadata"})
			return
		}

		workerAddr := location.Address
		client, err := h.getDataNodeClient(workerAddr)
		if err != nil {
			h.logger.Error("Failed to connect to datanode", zap.String("addr", workerAddr), zap.Error(err))
			c.JSON(http.StatusBadGateway, gin.H{"error": "Failed to connect to storage node"})
			return
		}

		h.logger.Debug("Fetching block", zap.String("block_id", blockID), zap.String("worker", workerAddr))
		blockData, err := client.FetchBlock(c.Request.Context(), blockID)
		if err != nil {
			h.logger.Error("Failed to fetch block", zap.String("block_id", blockID), zap.Error(err))
			c.JSON(http.StatusBadGateway, gin.H{"error": "Failed to retrieve data block"})
			return
		}

		fileBuffer.Write(blockData)
	}

	h.logger.Info("Successfully retrieved file", zap.String("file", fileName), zap.Int("size", fileBuffer.Len()))
	c.Data(http.StatusOK, "application/octet-stream", fileBuffer.Bytes())
}

func (h *QueryHandler) GetSchemaData(c *gin.Context) {
	ownerIdVal, exists := c.Get("userID")
	if !exists {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "Unauthorized: missing user ID"})
		return
	}
	ownerId := ownerIdVal.(string)

	projectID := c.Param("project")
	schemaName := c.Param("schema")

	h.logger.Info("Starting schema data aggregation",
		zap.String("project", projectID),
		zap.String("schema", schemaName),
		zap.String("owner", ownerId),
	)

	prefix := fmt.Sprintf("%s/", schemaName)

	fileListResp, err := h.MasterClient.ListFiles(c.Request.Context(), projectID, prefix)
	if err != nil {
		h.logger.Error("Failed to fetch file list", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to fetch file list"})
		return
	}

	var tableData []interface{}

	for _, fileName := range fileListResp.FilePaths {
		if !isStackPartOfSchema(fileName, schemaName) {
			continue
		}

		fullPath := filepath.Join(projectID, fileName)

		metadata, err := h.MasterClient.GetFileMetadata(c.Request.Context(), projectID, fullPath)
		if err != nil {
			h.logger.Warn("Skipping file due to metadata error (possibly compacted)", zap.String("file", fullPath), zap.Error(err))
			continue
		}

		var fileBuffer bytes.Buffer
		for _, blockInfo := range metadata.Blocks {
			location, ok := metadata.Locations[blockInfo.BlockId]
			if !ok {
				continue
			}

			workerAddr := location.Address
			client, err := h.getDataNodeClient(workerAddr)
			if err != nil {
				h.logger.Warn("Skipping block due to connection error", zap.String("worker", workerAddr))
				continue
			}

			data, err := client.FetchBlock(c.Request.Context(), blockInfo.BlockId)
			if err != nil {
				h.logger.Error("Failed to retrieve block", zap.String("block", blockInfo.BlockId), zap.Error(err))
				continue
			}
			fileBuffer.Write(data)
		}

		if fileBuffer.Len() == 0 {
			continue
		}

		var records []interface{}
		var parseErr error

		if strings.HasSuffix(fileName, ".parquet") {
			records, parseErr = parseParquetBytes(fileBuffer.Bytes())
		} else {
			records, parseErr = parseAvroOCF(fileBuffer.Bytes())
		}

		if parseErr != nil {
			h.logger.Warn("Failed to parse file", zap.String("file", fileName), zap.Error(parseErr))
			continue
		}

		tableData = append(tableData, records...)
	}

	h.logger.Info("Successfully aggregated schema data",
		zap.String("schema", schemaName),
		zap.Int("total_records", len(tableData)),
	)
	c.JSON(http.StatusOK, tableData)
}

func parseAvroOCF(data []byte) ([]interface{}, error) {
	reader := bytes.NewReader(data)
	ocfReader, err := goavro.NewOCFReader(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to create OCF reader: %w", err)
	}

	var rows []interface{}
	for ocfReader.Scan() {
		datum, err := ocfReader.Read()
		if err != nil {
			return nil, fmt.Errorf("failed to read avro record: %w", err)
		}
		rows = append(rows, datum)
	}

	if err := ocfReader.Err(); err != nil {
		return nil, fmt.Errorf("error during OCF scan: %w", err)
	}

	return rows, nil
}

func parseParquetBytes(data []byte) ([]interface{}, error) {
	memFile := &MemoryFile{Reader: bytes.NewReader(data)}

	pr, err := reader.NewParquetReader(memFile, nil, 4)
	if err != nil {
		return nil, fmt.Errorf("failed to create parquet reader: %w", err)
	}
	defer pr.ReadStop()

	num := int(pr.GetNumRows())
	if num == 0 {
		return []interface{}{}, nil
	}

	jsonRecords := make([]string, num)
	if err := pr.Read(&jsonRecords); err != nil {
		return nil, fmt.Errorf("failed to read parquet rows: %w", err)
	}

	results := make([]interface{}, 0, num)
	for _, jsonStr := range jsonRecords {
		var rec map[string]interface{}
		if err := json.Unmarshal([]byte(jsonStr), &rec); err == nil {
			results = append(results, rec)
		}
	}

	return results, nil
}
