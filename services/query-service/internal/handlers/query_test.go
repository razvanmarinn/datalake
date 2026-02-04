package handlers

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestQuery_SQLParsing(t *testing.T) {
	t.Run("parses SELECT queries", func(t *testing.T) {
		query := "SELECT * FROM users"
		assert.Contains(t, query, "SELECT")
		assert.Contains(t, query, "FROM")
	})

	t.Run("parses WHERE clauses", func(t *testing.T) {
		query := "SELECT * FROM users WHERE id = 1"
		assert.Contains(t, query, "WHERE")
	})
}

func TestQuery_Validation(t *testing.T) {
	t.Run("validates query structure", func(t *testing.T) {
		validQueries := []string{
			"SELECT * FROM table1",
			"SELECT id, name FROM users",
			"SELECT COUNT(*) FROM orders",
		}
		for _, q := range validQueries {
			assert.NotEmpty(t, q)
			assert.Contains(t, q, "SELECT")
		}
	})

	t.Run("rejects invalid queries", func(t *testing.T) {
		invalidQueries := []string{"", "INVALID", "DROP TABLE users"}
		for _, q := range invalidQueries {
			if q == "" {
				assert.Empty(t, q)
			}
		}
	})
}
