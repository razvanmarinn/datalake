package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
	kf "github.com/razvanmarinn/streaming_ingestion/internal/kafka"
	"github.com/segmentio/kafka-go"
)

func setupRouter(kf *kf.KafkaWriter) *gin.Engine {
	r := gin.Default()

	r.GET("/ping", func(c *gin.Context) {
		c.String(http.StatusOK, "pong")
	})

	r.POST("/ingest", func(c *gin.Context) {
		var msg IngestMessageBody
		if err := c.ShouldBindJSON(&msg); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		err := kf.EnsureTopicExists(context.Background(), kf.TopicResolver.ResolveTopic(msg.SchemaName))
		if err != nil {
			fmt.Printf("topic already exists %s\n", err)
		}

		// schemaURL := "http://schema-registry:8081/schemas/" + msg.SchemaName
		// resp, err := http.Get(schemaURL)
		// if err != nil || resp.StatusCode != http.StatusOK {
		// 	c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid schema name"})
		// 	return
		// }

		jsonData, _ := json.Marshal(msg.Data)

		kf.WriteMessageForSchema(context.Background(), msg.SchemaName, kafka.Message{
			Key:   []byte(msg.SchemaName),
			Value: jsonData,
		})

		c.JSON(http.StatusOK, gin.H{"status": "data received"})
	})

	return r
}

func main() {
	kafkaWriter := kf.NewKafkaWriter([]string{"localhost:9092"})
	r := setupRouter(kafkaWriter)
	r.Run(":8080")
}

type IngestMessageBody struct {
	SchemaName string                 `json:"schema_name" binding:"required"`
	Data       map[string]interface{} `json:"data" binding:"required"`
}
