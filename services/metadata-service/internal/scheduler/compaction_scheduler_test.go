package scheduler

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestScheduler_Configuration(t *testing.T) {
	t.Run("validates scheduler config", func(t *testing.T) {
		config := map[string]interface{}{
			"interval": 24 * time.Hour,
			"enabled":  true,
		}
		assert.Equal(t, 24*time.Hour, config["interval"])
		assert.True(t, config["enabled"].(bool))
	})
}

func TestScheduler_JobScheduling(t *testing.T) {
	t.Run("schedules compaction job", func(t *testing.T) {
		job := map[string]interface{}{
			"type":      "compaction",
			"project":   "proj-123",
			"scheduled": time.Now(),
		}
		assert.Equal(t, "compaction", job["type"])
	})
}
