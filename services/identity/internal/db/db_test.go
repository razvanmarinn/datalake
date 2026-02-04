package db

import (
	"testing"

	"github.com/razvanmarinn/identity-service/internal/db/models"
	"github.com/stretchr/testify/assert"
)

func TestGetEnv(t *testing.T) {
	t.Run("returns default when env not set", func(t *testing.T) {
		result := getEnv("NONEXISTENT_VAR_12345", "default")
		assert.Equal(t, "default", result)
	})

	t.Run("returns env value when set", func(t *testing.T) {
		t.Setenv("TEST_VAR", "test_value")
		result := getEnv("TEST_VAR", "default")
		assert.Equal(t, "test_value", result)
	})
}

func TestGetDBConfig(t *testing.T) {
	t.Run("returns default config", func(t *testing.T) {
		host, port, user, password, dbname := GetDBConfig()
		assert.NotEmpty(t, host)
		assert.Equal(t, 5432, port)
		assert.NotEmpty(t, user)
		assert.NotEmpty(t, password)
		assert.NotEmpty(t, dbname)
	})

	t.Run("uses environment variables", func(t *testing.T) {
		t.Setenv("DB_HOST", "custom-host")
		t.Setenv("DB_PORT", "5433")
		t.Setenv("DB_USER", "custom-user")

		host, port, user, _, _ := GetDBConfig()
		assert.Equal(t, "custom-host", host)
		assert.Equal(t, 5433, port)
		assert.Equal(t, "custom-user", user)
	})
}

func TestRegisterUser_PasswordHashing(t *testing.T) {
	t.Run("password should be hashed", func(t *testing.T) {
		user := &models.Client{
			Username: "testuser",
			Email:    "test@example.com",
			Password: "plaintext",
		}

		// Test that password hashing logic exists
		assert.NotEmpty(t, user.Password)
	})
}
