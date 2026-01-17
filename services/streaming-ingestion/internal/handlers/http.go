package handlers

import (
	"encoding/json"
	"net/http"
	"os"

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
	OwnerId    string                 `json:"owner_id"`
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
		logger.Info("Ingest handler invoked")
		ctx := c.Request.Context()
		tracer := otel.Tracer("streaming-ingestion")

		kf.Metrics.ActiveIngestionSessions.Inc()
		defer kf.Metrics.ActiveIngestionSessions.Dec()

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
		if hdr := c.GetHeader("X-User-ID"); hdr != "" {
			msg.OwnerId = hdr
		}

		logger.WithProject(msg.ProjectId).Info("Ingesting message for schema",
			zap.String("schema", msg.SchemaName),
			zap.String("owner", msg.OwnerId))
		ctx, child := tracer.Start(ctx, "EnsureTopicExists")

		child.End()

		ctx, child = tracer.Start(ctx, "WriteKafkaMessage")

		jsonData, _ := json.Marshal(msg)

		kf.Metrics.DataIngestionBytes.WithLabelValues(msg.ProjectId, "json").Add(float64(len(jsonData)))

		headers := make([]kafka.Header, 0)
		carrier := kafkaHeaderCarrier{headers: &headers}
		propagator.Inject(ctx, &carrier)

		topic := kf.TopicResolver.ResolveTopic(msg.ProjectId, msg.SchemaName)
		err := kf.WriteMessageForSchema(ctx, msg.ProjectId, msg.SchemaName, kafka.Message{
			Key:     []byte(msg.SchemaName),
			Value:   jsonData,
			Headers: headers,
		})

		if err != nil {
			logger.WithError(err).Error("Failed to produce message to Kafka",
				zap.String("topic", topic),
				zap.String("brokers", os.Getenv("KAFKA_BROKERS")))

			kf.Metrics.KafkaProduceErrors.WithLabelValues(topic, "write_error").Inc()
			child.RecordError(err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to write to Kafka"})
			child.End()
			return
		}
		child.End()

		c.JSON(http.StatusOK, gin.H{"status": "data received"})
	})
	return r
}
