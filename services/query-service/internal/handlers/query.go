package handlers

import (
	"bytes"
	"fmt"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/linkedin/goavro/v2"
	"github.com/razvanmarinn/query_service/internal/grpc"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
)

// QueryHandler is a handler for the query service.
type QueryHandler struct {
	logger        *zap.Logger
	MasterClient  *grpc.MasterClient
	WorkerClients map[string]*grpc.WorkerClient
	QueryCounter  metric.Int64Counter
}

// NewQueryHandler creates a new QueryHandler.
func NewQueryHandler(logger *zap.Logger, masterClient *grpc.MasterClient, workerClients map[string]*grpc.WorkerClient, queryCounter metric.Int64Counter) *QueryHandler {
	return &QueryHandler{
		logger:        logger,
		MasterClient:  masterClient,
		WorkerClients: workerClients,
		QueryCounter:  queryCounter,
	}
}

func (h *QueryHandler) GetFileList(c *gin.Context) {
	ownerIdVal, exists := c.Get("userID")
	if !exists {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "missing owner ID"})
		return
	}
	ownerId := ownerIdVal.(string)
	h.logger.Info("Owner id", zap.String("ownerId", ownerId))

	projectID := c.Param("project")
	if projectID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "missing project parameter"})
		return
	}

	h.logger.Info("Attempting GetFileListForProject RPC",
		zap.String("OwnerID", ownerId),
		zap.String("ProjectID", projectID),
	)

	resp, err := h.MasterClient.GetFileListForProject(
		c.Request.Context(),
		ownerId,
		projectID,
	)

	if err != nil {
		h.logger.Error("MasterClient RPC Failed", zap.Error(err))

		c.JSON(http.StatusInternalServerError, gin.H{"error": "Internal master service error: " + err.Error()})
		return
	}

	c.JSON(http.StatusOK, resp)
}

// GetData retrieves file data from the workers.
func (h *QueryHandler) GetData(c *gin.Context) {
	h.QueryCounter.Add(c.Request.Context(), 1)
	fileName := c.Query("file_name")
	if fileName == "" {
		h.logger.Error("file_name query parameter is required")
		c.JSON(http.StatusBadRequest, gin.H{"error": "file_name query parameter is required"})
		return
	}
	h.logger.Info("Received request for file", zap.String("file_name", fileName))

	metadata, err := h.MasterClient.GetMetadata(c.Request.Context(), fileName)
	if err != nil {
		h.logger.Error("Failed to get metadata from master", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	h.logger.Info("Successfully retrieved metadata", zap.Any("metadata", metadata))

	var data bytes.Buffer
	for _, batchID := range metadata.BatchIds {
		location, ok := metadata.BatchLocations[batchID]
		if !ok || len(location.WorkerIds) == 0 {
			h.logger.Warn("No location found for batch", zap.String("batch_id", batchID))
			continue // Or handle error
		}
		workerAddr := location.WorkerIds[0] // Simple strategy: use the first worker
		h.logger.Info("Retrieving batch from worker", zap.String("batch_id", batchID), zap.String("worker_address", workerAddr))
		workerClient, ok := h.WorkerClients[workerAddr]
		if !ok {
			h.logger.Error("Unknown worker address", zap.String("worker_address", workerAddr))
			c.JSON(http.StatusInternalServerError, gin.H{"error": "unknown worker address: " + workerAddr})
			return
		}

		batch, err := workerClient.RetrieveBatch(c.Request.Context(), batchID)
		if err != nil {
			h.logger.Error("Failed to retrieve batch from worker", zap.Error(err))
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		data.Write(batch.BatchData)
	}

	h.logger.Info("Successfully retrieved all batches for file", zap.String("file_name", fileName))
	c.Data(http.StatusOK, "application/octet-stream", data.Bytes())
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

	fileListResp, err := h.MasterClient.GetFileListForProject(c.Request.Context(), ownerId, projectID)
	if err != nil {
		h.logger.Error("Failed to fetch file list", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to fetch file list"})
		return
	}

	var tableData []interface{} 

	for _, fileName := range fileListResp.FileListNames {
		// Filter: Check if file belongs to the requested schema
		if !isStackPartOfSchema(fileName, projectID, schemaName) {
			continue
		}

		// A. Get Metadata
		metadata, err := h.MasterClient.GetMetadata(c.Request.Context(), fileName)
		if err != nil {
			h.logger.Warn("Skipping file due to metadata error", zap.String("file", fileName), zap.Error(err))
			continue
		}

		// B. Retrieve and Assemble File Bytes
		var fileBuffer bytes.Buffer
		for _, batchID := range metadata.BatchIds {
			location, ok := metadata.BatchLocations[batchID]
			if !ok || len(location.WorkerIds) == 0 {
				continue
			}

			workerAddr := location.WorkerIds[0]
			workerClient, ok := h.WorkerClients[workerAddr]
			if !ok {
				continue
			}

			batch, err := workerClient.RetrieveBatch(c.Request.Context(), batchID)
			if err != nil {
				h.logger.Error("Failed to retrieve batch", zap.String("batch", batchID), zap.Error(err))
				continue
			}
			fileBuffer.Write(batch.BatchData)
		}

		records, err := parseAvroOCF(fileBuffer.Bytes())
		if err != nil {
			h.logger.Warn("Failed to parse Avro file", zap.String("file", fileName), zap.Error(err))
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

func isStackPartOfSchema(filename, projectID string, schema string) bool {
	expectedPrefix := fmt.Sprintf("%s_%s_", projectID, schema)
    return strings.HasPrefix(filename, expectedPrefix)
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
