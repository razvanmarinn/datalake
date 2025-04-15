package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/razvanmarinn/streaming_ingestion/internal/handlers"
	kf "github.com/razvanmarinn/streaming_ingestion/internal/kafka"
)

func main() {
	kafkaBrokersStr := os.Getenv("KAFKA_BROKERS")
	if kafkaBrokersStr == "" {
		fmt.Println("Warning: KAFKA_BROKERS environment variable not set. Defaulting to localhost:9092.")
		kafkaBrokersStr = "localhost:9092"
	}
	kafkaBrokers := strings.Split(kafkaBrokersStr, ",")
	fmt.Printf("Using Kafka brokers: %v\n", kafkaBrokers)

	kafkaWriter := kf.NewKafkaWriter(kafkaBrokers)
	r := handlers.SetupRouter(kafkaWriter)
	r.Run(":8080")
}
