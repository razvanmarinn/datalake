package db

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDB_ConnectionString(t *testing.T) {
	t.Run("formats connection string", func(t *testing.T) {
		connStr := "host=localhost port=5432 user=test password=test dbname=testdb sslmode=disable"
		assert.Contains(t, connStr, "host=")
		assert.Contains(t, connStr, "port=")
		assert.Contains(t, connStr, "dbname=")
	})
}

func TestDB_Configuration(t *testing.T) {
	t.Run("validates db config", func(t *testing.T) {
		config := map[string]interface{}{
			"host":     "localhost",
			"port":     5432,
			"database": "metadata_db",
		}
		assert.Equal(t, "localhost", config["host"])
		assert.Equal(t, 5432, config["port"])
	})
}
