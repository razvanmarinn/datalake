package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/gin-gonic/gin"
	kf "github.com/razvanmarinn/streaming_ingestion/internal/kafka"
	"github.com/segmentio/kafka-go"
)

func setupRouter(kf *kf.KafkaWriter) *gin.Engine {
	r := gin.Default()

	r.GET("/ping", func(c *gin.Context) {
		c.String(http.StatusOK, "pong")
	})

	r.POST("/:project_name/ingest", func(c *gin.Context) {
		var msg IngestMessageBody
		project_name := c.Param("project_name")
		if err := c.ShouldBindJSON(&msg); err != nil {
			// Improved error handling with more information
			fmt.Printf("Error binding JSON: %v\n", err)
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		// Check if schema exists (you can add more detailed logging here)
		err := kf.EnsureTopicExists(context.Background(), kf.TopicResolver.ResolveTopic(msg.SchemaName))
		if err != nil {
			fmt.Printf("Error ensuring topic exists: %v\n", err)
		}

		// Convert the `Data` field to JSON for Kafka
		jsonData, _ := json.Marshal(msg)

		// Send message to Kafka
		kf.WriteMessageForSchema(context.Background(), project_name, kafka.Message{
			Key:   []byte(msg.SchemaName),
			Value: jsonData,
		})

		c.JSON(http.StatusOK, gin.H{"status": "data received"})
	})

	return r
}

func main() {
	// Get Kafka brokers from environment variable
	kafkaBrokersStr := os.Getenv("KAFKA_BROKERS")
	if kafkaBrokersStr == "" {
		fmt.Println("Warning: KAFKA_BROKERS environment variable not set. Defaulting to localhost:9092. This will likely not work with Kubernetes.")
		kafkaBrokersStr = "localhost:9092"
	}
	kafkaBrokers := strings.Split(kafkaBrokersStr, ",")
	fmt.Printf("Using Kafka brokers: %v\n", kafkaBrokers)

	kafkaWriter := kf.NewKafkaWriter(kafkaBrokers)
	r := setupRouter(kafkaWriter)
	r.Run(":8080")
}

type IngestMessageBody struct {
	SchemaName string                 `json:"schema_name" binding:"required"`
	Data       map[string]interface{} `json:"data" binding:"required"`
}