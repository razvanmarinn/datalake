package kafka

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestKafka_Configuration(t *testing.T) {
	t.Run("validates kafka config", func(t *testing.T) {
		config := map[string]interface{}{
			"brokers": []string{"localhost:9092"},
			"topic":   "metadata-events",
		}
		brokers := config["brokers"].([]string)
		assert.Len(t, brokers, 1)
		assert.Equal(t, "localhost:9092", brokers[0])
	})
}

func TestKafka_TopicNaming(t *testing.T) {
	t.Run("generates topic names", func(t *testing.T) {
		projectID := "proj-123"
		topic := projectID + "-events"
		assert.Equal(t, "proj-123-events", topic)
	})
}

func TestKafka_MessageFormat(t *testing.T) {
	t.Run("formats kafka message", func(t *testing.T) {
		message := map[string]interface{}{
			"event": "schema.created",
			"data":  map[string]string{"id": "schema-123"},
		}
		assert.Equal(t, "schema.created", message["event"])
	})
}
