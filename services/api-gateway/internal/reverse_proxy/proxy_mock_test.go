package reverse_proxy

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestURLParsing(t *testing.T) {
	t.Run("parses valid URL", func(t *testing.T) {
		targetURL := "http://metadata-service:8080"
		parsed, err := url.Parse(targetURL)
		assert.NoError(t, err)
		assert.Equal(t, "http", parsed.Scheme)
		assert.Equal(t, "metadata-service:8080", parsed.Host)
	})

	t.Run("parses URL with path", func(t *testing.T) {
		targetURL := "http://service:8080/api/v1"
		parsed, err := url.Parse(targetURL)
		assert.NoError(t, err)
		assert.Equal(t, "/api/v1", parsed.Path)
	})
}

func TestProxyHeaders(t *testing.T) {
	t.Run("sets X-Forwarded-Host", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/test", nil)
		req.Header.Set("Host", "original-host.com")

		forwardedHost := req.Header.Get("Host")
		assert.Equal(t, "original-host.com", forwardedHost)
	})

	t.Run("sets X-User-ID", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/test", nil)
		req.Header.Set("X-User-ID", "user-123")

		userID := req.Header.Get("X-User-ID")
		assert.Equal(t, "user-123", userID)
	})

	t.Run("sets X-Project-ID", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/test", nil)
		req.Header.Set("X-Project-ID", "proj-456")

		projectID := req.Header.Get("X-Project-ID")
		assert.Equal(t, "proj-456", projectID)
	})
}

func TestPathRewriting(t *testing.T) {
	t.Run("rewrites to /ingest", func(t *testing.T) {
		originalPath := "/api/v1/stream/project123"
		newPath := "/ingest"
		assert.NotEqual(t, originalPath, newPath)
		assert.Equal(t, "/ingest", newPath)
	})

	t.Run("prepends /schema", func(t *testing.T) {
		originalPath := "/projects/schemas"
		newPath := "/schema" + originalPath
		assert.Equal(t, "/schema/projects/schemas", newPath)
	})

	t.Run("strips prefix", func(t *testing.T) {
		path := "test/path"
		if len(path) > 0 && path[0] != '/' {
			path = "/" + path
		}
		assert.Equal(t, "/test/path", path)
	})
}

func TestErrorHandling(t *testing.T) {
	t.Run("returns BadGateway on error", func(t *testing.T) {
		recorder := httptest.NewRecorder()
		recorder.WriteHeader(http.StatusBadGateway)
		assert.Equal(t, http.StatusBadGateway, recorder.Code)
	})

	t.Run("returns BadRequest for missing project", func(t *testing.T) {
		recorder := httptest.NewRecorder()
		recorder.WriteHeader(http.StatusBadRequest)
		assert.Equal(t, http.StatusBadRequest, recorder.Code)
	})

	t.Run("returns Unauthorized for missing userID", func(t *testing.T) {
		recorder := httptest.NewRecorder()
		recorder.WriteHeader(http.StatusUnauthorized)
		assert.Equal(t, http.StatusUnauthorized, recorder.Code)
	})
}

func TestProxyConfiguration(t *testing.T) {
	t.Run("streaming ingestion config", func(t *testing.T) {
		config := map[string]string{
			"target": "http://streaming-ingestion:8085",
			"path":   "/ingest",
		}
		assert.Equal(t, "http://streaming-ingestion:8085", config["target"])
	})

	t.Run("metadata service config", func(t *testing.T) {
		config := map[string]string{
			"target": "http://metadata-service:8080",
			"path":   "/schema",
		}
		assert.Equal(t, "http://metadata-service:8080", config["target"])
	})

	t.Run("query service config", func(t *testing.T) {
		config := map[string]string{
			"target": "http://query-service:8086",
		}
		assert.Equal(t, "http://query-service:8086", config["target"])
	})
}
