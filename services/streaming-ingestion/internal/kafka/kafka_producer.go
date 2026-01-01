package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/razvanmarinn/datalake/pkg/metrics"
	"github.com/segmentio/kafka-go"
)

type KafkaWriter struct {
	Writer        *kafka.Writer
	TopicResolver *KafkaTopicResolver // Changed to pointer for consistency
	Brokers       []string
	Metrics       *metrics.StreamingMetrics
}

type KafkaTopicResolver struct{}

func NewKafkaTopicResolver() *KafkaTopicResolver {
	return &KafkaTopicResolver{}
}

// FIX: Generate a topic name that satisfies the consumer regex "^.+\\..+"
func (k *KafkaTopicResolver) ResolveTopic(projectName string) string {
	// Example: "datalake.project-razv"
	return fmt.Sprintf("datalake.%s", projectName)
}

func NewKafkaWriter(brokers []string) *KafkaWriter {
	return &KafkaWriter{
		Writer: kafka.NewWriter(kafka.WriterConfig{
			Brokers:      brokers,
			Balancer:     &kafka.LeastBytes{},
			Async:        false, // Set to true for higher throughput
			BatchSize:    10,
			BatchTimeout: 1 * time.Second,
			RequiredAcks: 1,
		}),
		TopicResolver: NewKafkaTopicResolver(), // Initialize the resolver
		Brokers:       brokers,
	}
}

func (k *KafkaWriter) SetMetrics(metrics *metrics.StreamingMetrics) {
	k.Metrics = metrics
}

func (k *KafkaWriter) WriteMessageForSchema(ctx context.Context, projectName string, message kafka.Message) error {
	topic := k.TopicResolver.ResolveTopic(projectName)

	message.Topic = topic

	if k.Metrics != nil {
		k.Metrics.KafkaMessagesBatchSize.WithLabelValues(topic).Observe(1)
	}

	return k.Writer.WriteMessages(ctx, message)
}
