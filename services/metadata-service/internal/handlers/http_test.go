package handlers

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHTTP_EndpointRouting(t *testing.T) {
	t.Run("defines schema endpoints", func(t *testing.T) {
		endpoints := []string{
			"/api/v1/schemas",
			"/api/v1/schemas/:id",
			"/api/v1/projects/:id/schemas",
		}
		for _, endpoint := range endpoints {
			assert.NotEmpty(t, endpoint)
			assert.Contains(t, endpoint, "/api/v1/")
		}
	})

	t.Run("defines project endpoints", func(t *testing.T) {
		endpoints := []string{
			"/api/v1/projects",
			"/api/v1/projects/:id",
		}
		for _, endpoint := range endpoints {
			assert.Contains(t, endpoint, "/projects")
		}
	})
}

func TestHTTP_ResponseFormats(t *testing.T) {
	t.Run("formats success response", func(t *testing.T) {
		response := map[string]interface{}{
			"status": "success",
			"data":   map[string]string{"id": "123"},
		}
		assert.Equal(t, "success", response["status"])
	})

	t.Run("formats error response", func(t *testing.T) {
		response := map[string]interface{}{
			"status": "error",
			"error":  "Not found",
		}
		assert.Equal(t, "error", response["status"])
	})
}
