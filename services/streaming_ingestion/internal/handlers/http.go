package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	kf "github.com/razvanmarinn/streaming_ingestion/internal/kafka"

	"github.com/gin-gonic/gin"
	"github.com/segmentio/kafka-go"
)

type IngestMessageBody struct {
	SchemaName string                 `json:"schema_name" binding:"required"`
	Data       map[string]interface{} `json:"data" binding:"required"`
}

func SetupRouter(kf *kf.KafkaWriter) *gin.Engine {
	r := gin.Default()

	r.POST("/:project_name/ingest", func(c *gin.Context) {
		var msg IngestMessageBody
		project_name := c.Param("project_name")
		if err := c.ShouldBindJSON(&msg); err != nil {
			fmt.Printf("Error binding JSON: %v\n", err)
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		topicExists, err := kf.EnsureTopicExists(context.Background(), kf.TopicResolver.ResolveTopic(msg.SchemaName))
		if err != nil {
			fmt.Printf("Error ensuring topic exists: %v\n", err)
		}
		if !topicExists {
			fmt.Printf("Topic %s does not exist\n", msg.SchemaName)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Topic does not exist"})
			return
		}
		jsonData, _ := json.Marshal(msg)

		kf.WriteMessageForSchema(context.Background(), project_name, kafka.Message{
			Key:   []byte(msg.SchemaName),
			Value: jsonData,
		})

		c.JSON(http.StatusOK, gin.H{"status": "data received"})
	})

	return r
}
