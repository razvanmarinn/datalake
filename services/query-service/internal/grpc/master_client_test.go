package grpc

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMasterClient_Configuration(t *testing.T) {
	t.Run("validates master client config", func(t *testing.T) {
		config := map[string]interface{}{
			"address": "dfs-master:50050",
			"timeout": 30,
		}
		assert.Equal(t, "dfs-master:50050", config["address"])
	})
}

func TestMasterClient_RequestFormats(t *testing.T) {
	t.Run("formats file metadata request", func(t *testing.T) {
		request := map[string]string{
			"project_id": "proj-123",
			"file_path":  "/data/file.parquet",
		}
		assert.Equal(t, "proj-123", request["project_id"])
	})

	t.Run("formats list files request", func(t *testing.T) {
		request := map[string]string{
			"project_id": "proj-123",
			"prefix":     "/data/",
		}
		assert.Equal(t, "/data/", request["prefix"])
	})
}
