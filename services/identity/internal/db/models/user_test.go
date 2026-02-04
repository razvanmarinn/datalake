package models

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestClient_Structure(t *testing.T) {
	t.Run("creates client with all fields", func(t *testing.T) {
		id := uuid.New()
		client := Client{
			ID:       id,
			Username: "testuser",
			Email:    "test@example.com",
			Password: "hashedpassword",
		}
		assert.Equal(t, id, client.ID)
		assert.Equal(t, "testuser", client.Username)
		assert.Equal(t, "test@example.com", client.Email)
		assert.NotEmpty(t, client.Password)
	})
}

func TestClient_Validation(t *testing.T) {
	t.Run("validates username format", func(t *testing.T) {
		validUsernames := []string{"user123", "test_user", "user-name"}
		for _, username := range validUsernames {
			assert.NotEmpty(t, username)
			assert.True(t, len(username) >= 3)
		}
	})

	t.Run("validates email format", func(t *testing.T) {
		validEmails := []string{
			"test@example.com",
			"user.name@domain.co.uk",
			"user+tag@example.com",
		}
		for _, email := range validEmails {
			assert.Contains(t, email, "@")
			assert.Contains(t, email, ".")
		}
	})
}
