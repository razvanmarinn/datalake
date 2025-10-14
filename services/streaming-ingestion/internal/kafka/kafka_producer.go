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
	TopicResolver KafkaTopicResolver
	Brokers       []string
	Metrics       *metrics.StreamingMetrics
}

type KafkaTopicResolver struct{}

func NewKafkaTopicResolver() *KafkaTopicResolver {
	return &KafkaTopicResolver{}
}

func (k *KafkaTopicResolver) ResolveTopic(projectName string) string {
	return "raw_test1"
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
		Brokers: brokers,
	}
}

func (k *KafkaWriter) SetMetrics(metrics *metrics.StreamingMetrics) {
	k.Metrics = metrics
}

func (k *KafkaWriter) EnsureTopicExists(ctx context.Context, topic string) (bool, error) {
	conn, err := kafka.Dial("tcp", k.Brokers[0])
	if err != nil {
		return false, fmt.Errorf("failed to connect to Kafka: %w", err)
	}
	defer conn.Close()

	partitions, err := conn.ReadPartitions(topic)
	if err == nil && len(partitions) > 0 {
		return true, nil // Topic already exists
	}
	return false, nil
}

func (k *KafkaWriter) WriteMessageForSchema(ctx context.Context, projectName string, message kafka.Message) error {
	topic := k.TopicResolver.ResolveTopic(projectName)

	topicMessage := message
	topicMessage.Topic = topic

	if k.Metrics != nil {
		k.Metrics.KafkaMessagesBatchSize.WithLabelValues(topic).Observe(1) // Single message batch
	}

	return k.Writer.WriteMessages(ctx, topicMessage)
}
