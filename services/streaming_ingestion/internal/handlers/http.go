package handlers

import (
	"encoding/json"
	"net/http"

	kf "github.com/razvanmarinn/streaming_ingestion/internal/kafka"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	"github.com/gin-gonic/gin"
	"github.com/segmentio/kafka-go"
)

type IngestMessageBody struct {
	SchemaName string                 `json:"schema_name" binding:"required"`
	ProjectId  string                 `json:"project_id"`
	Data       map[string]interface{} `json:"data" binding:"required"`
}

func SetupRouter(r *gin.Engine, kf *kf.KafkaWriter) *gin.Engine {
	r.POST("/ingest", func(c *gin.Context) {
		ctx := c.Request.Context()
		tracer := otel.Tracer("streaming-ingestion")

		// ★ Child span for full handler logic
		ctx, span := tracer.Start(ctx, "HandleIngestRequest", trace.WithSpanKind(trace.SpanKindInternal))
		defer span.End()

		var msg IngestMessageBody
		if err := c.ShouldBindJSON(&msg); err != nil {
			span.RecordError(err)
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		if hdr := c.GetHeader("X-Project-ID"); hdr != "" {
			msg.ProjectId = hdr
		}

		// ★ Child span for topic validation
		func() {
			ctx, child := tracer.Start(ctx, "EnsureTopicExists")
			defer child.End()

			topicExists, err := kf.EnsureTopicExists(ctx, kf.TopicResolver.ResolveTopic(msg.SchemaName))
			if err != nil {
				child.RecordError(err)
			}
			if !topicExists {
				c.JSON(http.StatusInternalServerError, gin.H{"error": "Topic does not exist"})
			}
		}()

		// ★ Child span for Kafka write
		func() {
			ctx, child := tracer.Start(ctx, "WriteKafkaMessage")
			defer child.End()

			jsonData, _ := json.Marshal(msg)
			kf.WriteMessageForSchema(ctx, msg.ProjectId, kafka.Message{
				Key:   []byte(msg.SchemaName),
				Value: jsonData,
			})
		}()

		c.JSON(http.StatusOK, gin.H{"status": "data received"})
	})
	return r
}
