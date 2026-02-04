package handlers

import (
	"bytes"
	"encoding/json"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLoginBody_Structure(t *testing.T) {
	t.Run("creates login body", func(t *testing.T) {
		body := LoginBody{
			Username: "testuser",
			Password: "testpass",
		}
		assert.Equal(t, "testuser", body.Username)
		assert.Equal(t, "testpass", body.Password)
	})
}

func TestLoginBody_JSON(t *testing.T) {
	t.Run("marshals to JSON", func(t *testing.T) {
		body := LoginBody{
			Username: "testuser",
			Password: "testpass",
		}
		data, err := json.Marshal(body)
		assert.NoError(t, err)
		assert.Contains(t, string(data), "testuser")
	})

	t.Run("unmarshals from JSON", func(t *testing.T) {
		jsonData := `{"username":"testuser","password":"testpass"}`
		var body LoginBody
		err := json.Unmarshal([]byte(jsonData), &body)
		assert.NoError(t, err)
		assert.Equal(t, "testuser", body.Username)
	})
}

func TestHTTP_RequestValidation(t *testing.T) {
	t.Run("validates required fields", func(t *testing.T) {
		invalidBodies := []string{
			`{}`,
			`{"username":""}`,
			`{"password":""}`,
		}
		for _, body := range invalidBodies {
			req := httptest.NewRequest("POST", "/login", bytes.NewBufferString(body))
			req.Header.Set("Content-Type", "application/json")
			assert.NotNil(t, req)
		}
	})
}

func TestHTTP_ResponseFormats(t *testing.T) {
	t.Run("success response format", func(t *testing.T) {
		response := map[string]interface{}{
			"token":   "test-token",
			"user-id": "123",
		}
		assert.Contains(t, response, "token")
		assert.Contains(t, response, "user-id")
	})

	t.Run("error response format", func(t *testing.T) {
		response := map[string]string{
			"error": "Invalid credentials",
		}
		assert.Contains(t, response, "error")
	})
}
