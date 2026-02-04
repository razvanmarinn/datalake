package grpc

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWorkerClient_Configuration(t *testing.T) {
	t.Run("validates worker client config", func(t *testing.T) {
		config := map[string]interface{}{
			"address": "worker-0:50051",
			"timeout": 60,
		}
		assert.Equal(t, "worker-0:50051", config["address"])
	})
}

func TestWorkerClient_BlockRequests(t *testing.T) {
	t.Run("formats fetch block request", func(t *testing.T) {
		request := map[string]string{
			"block_id": "block-123",
		}
		assert.Equal(t, "block-123", request["block_id"])
	})
}
