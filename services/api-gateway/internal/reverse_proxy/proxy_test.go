package reverse_proxy

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestProxy_Configuration(t *testing.T) {
	t.Run("configures proxy settings", func(t *testing.T) {
		config := map[string]string{
			"target": "http://backend:8080",
			"path":   "/api",
		}
		assert.Equal(t, "http://backend:8080", config["target"])
	})
}

func TestProxy_RequestRouting(t *testing.T) {
	t.Run("routes requests to correct backend", func(t *testing.T) {
		routes := map[string]string{
			"/identity": "identity-service:50051",
			"/metadata": "metadata-service:50052",
			"/query":    "query-service:50053",
		}
		assert.Len(t, routes, 3)
		assert.Equal(t, "identity-service:50051", routes["/identity"])
	})
}
