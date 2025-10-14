package handlers

import (
	"encoding/json"
	"net/http"

	"github.com/razvanmarinn/datalake/pkg/logging"
	"go.uber.org/zap"

	"github.com/gin-gonic/gin"
	kf "github.com/razvanmarinn/streaming_ingestion/internal/kafka"
	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

var propagator = propagation.NewCompositeTextMapPropagator(
	propagation.TraceContext{},
	propagation.Baggage{},
)

type IngestMessageBody struct {
	SchemaName string                 `json:"schema_name" binding:"required"`
	ProjectId  string                 `json:"project_id"`
	Data       map[string]interface{} `json:"data" binding:"required"`
}
type kafkaHeaderCarrier struct {
	headers *[]kafka.Header
}

func (c *kafkaHeaderCarrier) Get(key string) string {
	for _, h := range *c.headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

func (c *kafkaHeaderCarrier) Set(key, val string) {
	for i, h := range *c.headers {
		if h.Key == key {
			(*c.headers)[i].Value = []byte(val)
			return
		}
	}
	*c.headers = append(*c.headers, kafka.Header{
		Key:   key,
		Value: []byte(val),
	})
}

func (c *kafkaHeaderCarrier) Keys() []string {
	keys := make([]string, len(*c.headers))
	for i, h := range *c.headers {
		keys[i] = h.Key
	}
	return keys
}

func SetupRouter(r *gin.Engine, kf *kf.KafkaWriter, logger *logging.Logger) *gin.Engine {
	r.POST("/ingest", func(c *gin.Context) {
		ctx := c.Request.Context()
		tracer := otel.Tracer("streaming-ingestion")

		kf.Metrics.ActiveIngestionSessions.Inc()
		defer kf.Metrics.ActiveIngestionSessions.Dec()

		// ★ Child span for full handler logic
		ctx, span := tracer.Start(ctx, "HandleIngestRequest", trace.WithSpanKind(trace.SpanKindInternal))
		defer span.End()

		var msg IngestMessageBody
		if err := c.ShouldBindJSON(&msg); err != nil {
			span.RecordError(err)
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			logger.WithError(err).Error("Failed to bind JSON in /ingest")
			return
		}

		if hdr := c.GetHeader("X-Project-ID"); hdr != "" {
			msg.ProjectId = hdr
		}

		func() {
			logger.WithProject(msg.ProjectId).Info("Ingesting message for schema", zap.String("schema", msg.SchemaName))
			ctx, child := tracer.Start(ctx, "EnsureTopicExists")
			defer child.End()

			topicExists, err := kf.EnsureTopicExists(ctx, kf.TopicResolver.ResolveTopic(msg.SchemaName))
			if err != nil {
				child.RecordError(err)
				c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to verify topic"})
				logger.WithError(err).Error("Failed to verify topic in /ingest")
				return
			}
			if !topicExists {
				c.JSON(http.StatusInternalServerError, gin.H{"error": "Topic does not exist"})
				logger.Error("Topic does not exist in /ingest", zap.String("topic", kf.TopicResolver.ResolveTopic(msg.SchemaName)))
			}
		}()

		// ★ Child span for Kafka write
		func() {
			ctx, child := tracer.Start(ctx, "WriteKafkaMessage")
			defer child.End()

			jsonData, _ := json.Marshal(msg)

			// Record data ingestion metrics
			kf.Metrics.DataIngestionBytes.WithLabelValues(msg.ProjectId, "json").Add(float64(len(jsonData)))

			// Inject trace context into Kafka headers
			headers := make([]kafka.Header, 0)
			carrier := kafkaHeaderCarrier{headers: &headers}
			propagator.Inject(ctx, &carrier)

			topic := kf.TopicResolver.ResolveTopic(msg.SchemaName)
			err := kf.WriteMessageForSchema(ctx, msg.ProjectId, kafka.Message{
				Key:     []byte(msg.SchemaName),
				Value:   jsonData,
				Headers: headers,
			})

			if err != nil {
				kf.Metrics.KafkaProduceErrors.WithLabelValues(topic, "write_error").Inc()
				child.RecordError(err)
				c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to write to Kafka"})
				return
			} else {
				kf.Metrics.KafkaMessagesProduced.WithLabelValues(topic).Inc()
			}
		}()

		c.JSON(http.StatusOK, gin.H{"status": "data received"})
	})
	return r
}
