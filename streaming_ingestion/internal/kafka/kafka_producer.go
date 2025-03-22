package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
)

type KafkaWriter struct {
	Writer        *kafka.Writer
	TopicResolver KafkaTopicResolver
	Brokers       []string
}

type KafkaTopicResolver struct{}

func NewKafkaTopicResolver() *KafkaTopicResolver {
	return &KafkaTopicResolver{}
}

func (k *KafkaTopicResolver) ResolveTopic(schemaName string) string {
	return "raw_" + schemaName
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

func (k *KafkaWriter) EnsureTopicExists(ctx context.Context, topic string) error {
	conn, err := kafka.Dial("tcp", k.Brokers[0])
	if err != nil {
		return fmt.Errorf("failed to connect to Kafka: %w", err)
	}
	defer conn.Close()

	partitions, err := conn.ReadPartitions(topic)
	if err == nil && len(partitions) > 0 {
		return nil // Topic already exists
	}

	return conn.CreateTopics(kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     3,
		ReplicationFactor: 1,
	})
}

func (k *KafkaWriter) WriteMessageForSchema(ctx context.Context, schemaName string, message kafka.Message) error {
	topic := k.TopicResolver.ResolveTopic(schemaName)


	topicMessage := message
	topicMessage.Topic = topic

	return k.Writer.WriteMessages(ctx, topicMessage)
}
