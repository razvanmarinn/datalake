package handlers

import (
	"bytes"
	"net/http"

	"github.com/gin-gonic/gin"
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
