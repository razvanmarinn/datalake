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
	TopicResolver *KafkaTopicResolver
	Brokers       []string
	Metrics       *metrics.StreamingMetrics
}

type KafkaTopicResolver struct{}

func NewKafkaTopicResolver() *KafkaTopicResolver {
	return &KafkaTopicResolver{}
}

func (k *KafkaTopicResolver) ResolveTopic(projectName string, schemaName string) string {
	return fmt.Sprintf("%s-%s", projectName, schemaName)
}

func NewKafkaWriter(brokers []string) *KafkaWriter {
	return &KafkaWriter{
		Writer: kafka.NewWriter(kafka.WriterConfig{
			Brokers:      brokers,
			Balancer:     &kafka.LeastBytes{},
			Async:        false,
			BatchSize:    10,
			BatchTimeout: 1 * time.Second,
			RequiredAcks: 1,
		}),
		TopicResolver: NewKafkaTopicResolver(),
		Brokers:       brokers,
	}
}

func (k *KafkaWriter) WriteMessageForSchema(ctx context.Context, projectName string, schemaName string, message kafka.Message) error {
	topic := k.TopicResolver.ResolveTopic(projectName, schemaName)

	message.Topic = topic

	if k.Metrics != nil {
		k.Metrics.KafkaMessagesBatchSize.WithLabelValues(topic).Observe(1)
	}

	return k.Writer.WriteMessages(ctx, message)
}

func (k *KafkaWriter) SetMetrics(metrics *metrics.StreamingMetrics) {
	k.Metrics = metrics
}
