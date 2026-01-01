package config

import (
	"errors"
	"os"
	"strings"
)

type Config struct {
	KafkaBrokers       []string
	KafkaTopicRegex    string
	KafkaGroupID       string
	KafkaDLTTopic      string
	SchemaRegistryHost string
	SchemaProjectName  string
	MasterAddress      string
	OtelCollectorAddr  string
}

func Load() (Config, error) {
	kafkaBrokers := os.Getenv("KAFKA_BROKERS")
	if kafkaBrokers == "" {
		return Config{}, errors.New("KAFKA_BROKERS must be set")
	}

	topicRegex := os.Getenv("KAFKA_TOPIC_REGEX")
	if topicRegex == "" {
		topicRegex = "^.+\\..+"
	}

	groupID := os.Getenv("KAFKA_GROUP_ID")
	if groupID == "" {
		return Config{}, errors.New("KAFKA_GROUP_ID must be set")
	}

	return Config{
		KafkaBrokers:    strings.Split(kafkaBrokers, ","),
		KafkaTopicRegex: topicRegex, 
		KafkaGroupID:    groupID,
		KafkaDLTTopic:      getEnv("KAFKA_DLT_TOPIC", "ingestion-dlt"),
		SchemaRegistryHost: getEnv("SCHEMA_REGISTRY_HOST", "schema-registry.datalake:8081"),
		SchemaProjectName:  getEnv("SCHEMA_PROJECT_NAME", "razvan"),
		MasterAddress:      getEnv("MASTER_ADDRESS", "master:50055"),
		OtelCollectorAddr:  getEnv("OTEL_COLLECTOR_ADDR", "otel-collector.observability.svc.cluster.local:4317"),
	}, nil
}

func getEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}
