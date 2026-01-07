package handlers

import (
	"bytes"
	"fmt"
	"net/http"
	"strings"
	"sync"

	"github.com/gin-gonic/gin"
	"github.com/linkedin/goavro/v2"
	"github.com/razvanmarinn/query_service/internal/grpc"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
)

// QueryHandler is a handler for the query service.
type QueryHandler struct {
	logger       *zap.Logger
	MasterClient *grpc.MasterClient

	// Cache for DataNode clients to avoid re-dialing constantly
	dataNodeClientsMu sync.Mutex
	dataNodeClients   map[string]*grpc.DataNodeClient

	QueryCounter metric.Int64Counter
}

// NewQueryHandler creates a new QueryHandler.
func NewQueryHandler(logger *zap.Logger, masterClient *grpc.MasterClient, queryCounter metric.Int64Counter) *QueryHandler {
	return &QueryHandler{
		logger:          logger,
		MasterClient:    masterClient,
		dataNodeClients: make(map[string]*grpc.DataNodeClient),
		QueryCounter:    queryCounter,
	}
}

// getDataNodeClient returns an existing client or creates a new one for the given address.
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
	// 1. Get Project ID
	projectID := c.Param("project")
	if projectID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "missing project parameter"})
		return
	}

	// 2. Optional: Directory Prefix
	prefix := c.Query("prefix")

	h.logger.Info("Attempting ListFiles RPC", zap.String("ProjectID", projectID))

	// 3. Call Coordinator
	resp, err := h.MasterClient.ListFiles(c.Request.Context(), projectID, prefix)
	if err != nil {
		h.logger.Error("MasterClient RPC Failed", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Internal master service error: " + err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"files": resp.FilePaths})
}

// GetData retrieves file data by fetching blocks from DataNodes.
func (h *QueryHandler) GetData(c *gin.Context) {
	h.QueryCounter.Add(c.Request.Context(), 1)

	// 1. Parse Parameters
	projectID := c.Param("project") // Assuming route is /:project/data
	fileName := c.Query("file_name")

	if projectID == "" || fileName == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "project and file_name are required"})
		return
	}

	h.logger.Info("Received request for file", zap.String("project", projectID), zap.String("file", fileName))

	// 2. Get Metadata from Coordinator
	metadata, err := h.MasterClient.GetFileMetadata(c.Request.Context(), projectID, fileName)
	if err != nil {
		h.logger.Error("Failed to get metadata", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	// 3. Fetch Blocks from DataNodes
	var fileBuffer bytes.Buffer

	// Iterate over blocks in order
	for _, blockInfo := range metadata.Blocks {
		blockID := blockInfo.BlockId

		// Find location for this block
		location, ok := metadata.Locations[blockID]
		if !ok {
			h.logger.Error("No location found for block", zap.String("block_id", blockID))
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Incomplete file metadata"})
			return
		}

		// Construct Worker Address
		workerAddr := location.Address

		// Get Client
		client, err := h.getDataNodeClient(workerAddr)
		if err != nil {
			h.logger.Error("Failed to connect to datanode", zap.String("addr", workerAddr), zap.Error(err))
			c.JSON(http.StatusBadGateway, gin.H{"error": "Failed to connect to storage node"})
			return
		}

		// Fetch Data
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

	// 1. List Files
	// Optimization: Pass schema name as prefix if your naming convention supports it (e.g. "project_schema_")
	prefix := fmt.Sprintf("%s_%s", projectID, schemaName)
	fileListResp, err := h.MasterClient.ListFiles(c.Request.Context(), projectID, prefix)
	if err != nil {
		h.logger.Error("Failed to fetch file list", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to fetch file list"})
		return
	}

	var tableData []interface{}

	for _, fileName := range fileListResp.FilePaths {
		// Double check schema match (in case prefix match was partial)
		if !isStackPartOfSchema(fileName, projectID, schemaName) {
			continue
		}

		// 2. Get Metadata
		metadata, err := h.MasterClient.GetFileMetadata(c.Request.Context(), projectID, fileName)
		if err != nil {
			h.logger.Warn("Skipping file due to metadata error", zap.String("file", fileName), zap.Error(err))
			continue
		}

		// 3. Assemble File
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

		// 4. Parse Avro
		if fileBuffer.Len() > 0 {
			records, err := parseAvroOCF(fileBuffer.Bytes())
			if err != nil {
				h.logger.Warn("Failed to parse Avro file", zap.String("file", fileName), zap.Error(err))
				continue
			}
			tableData = append(tableData, records...)
		}
	}

	h.logger.Info("Successfully aggregated schema data",
		zap.String("schema", schemaName),
		zap.Int("total_records", len(tableData)),
	)
	c.JSON(http.StatusOK, tableData)
}

func isStackPartOfSchema(filename, projectID string, schema string) bool {
	expectedPrefix := fmt.Sprintf("%s_%s", projectID, schema)
	return strings.Contains(filename, expectedPrefix)
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
