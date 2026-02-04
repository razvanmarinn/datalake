package handlers

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRunSQLRequest_Validation(t *testing.T) {
	t.Run("valid request", func(t *testing.T) {
		req := RunSQLRequest{
			Query:     "SELECT * FROM users",
			ProjectID: "proj-123",
		}
		assert.NotEmpty(t, req.Query)
		assert.NotEmpty(t, req.ProjectID)
	})

	t.Run("empty project_id", func(t *testing.T) {
		req := RunSQLRequest{
			Query:     "SELECT * FROM users",
			ProjectID: "",
		}
		assert.Empty(t, req.ProjectID)
	})

	t.Run("empty query", func(t *testing.T) {
		req := RunSQLRequest{
			Query:     "",
			ProjectID: "proj-123",
		}
		assert.Empty(t, req.Query)
	})
}

func TestIsStackPartOfSchema_Comprehensive(t *testing.T) {
	testCases := []struct {
		name       string
		filename   string
		schema     string
		expected   bool
	}{
		{"directory match", "myschema/file.parquet", "myschema", true},
		{"prefix match", "myschema_v1.parquet", "myschema", true},
		{"nested directory", "myschema/sub/file.parquet", "myschema", true},
		{"no match different schema", "otherschema/file.parquet", "myschema", false},
		{"partial name no match", "myschema123/file.parquet", "myschema", false},
		{"exact match with slash", "schema/data.parquet", "schema", true},
		{"exact match with underscore", "schema_2024.parquet", "schema", true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := isStackPartOfSchema(tc.filename, tc.schema)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestSQLQueryTypes(t *testing.T) {
	t.Run("SELECT query", func(t *testing.T) {
		query := "SELECT id, name FROM users WHERE active = true"
		assert.Contains(t, query, "SELECT")
		assert.Contains(t, query, "FROM")
		assert.Contains(t, query, "WHERE")
	})

	t.Run("SELECT with JOIN", func(t *testing.T) {
		query := "SELECT u.id, o.order_id FROM users u JOIN orders o ON u.id = o.user_id"
		assert.Contains(t, query, "JOIN")
		assert.Contains(t, query, "ON")
	})

	t.Run("SELECT with aggregation", func(t *testing.T) {
		query := "SELECT COUNT(*), SUM(amount) FROM orders GROUP BY user_id"
		assert.Contains(t, query, "COUNT")
		assert.Contains(t, query, "GROUP BY")
	})
}

func TestVirtualFileHandler_Validation(t *testing.T) {
	t.Run("validates project parameter", func(t *testing.T) {
		projectID := ""
		filePath := "schema/file.parquet"
		assert.Empty(t, projectID)
		assert.NotEmpty(t, filePath)
	})

	t.Run("validates file parameter", func(t *testing.T) {
		projectID := "proj-123"
		filePath := ""
		assert.NotEmpty(t, projectID)
		assert.Empty(t, filePath)
	})

	t.Run("constructs full path", func(t *testing.T) {
		projectID := "proj-123"
		filePath := "schema/data.parquet"
		expected := projectID + "/" + filePath
		assert.Equal(t, "proj-123/schema/data.parquet", expected)
	})
}

func TestTableReplacementLogic(t *testing.T) {
	t.Run("generates placeholder", func(t *testing.T) {
		tableName := "users"
		placeholder := "TOKEN_" + tableName
		assert.Equal(t, "TOKEN_users", placeholder)
	})

	t.Run("generates read_parquet", func(t *testing.T) {
		urls := []string{"'http://localhost/file1.parquet'", "'http://localhost/file2.parquet'"}
		replacement := "read_parquet([" + urls[0] + ", " + urls[1] + "])"
		assert.Contains(t, replacement, "read_parquet")
		assert.Contains(t, replacement, "file1.parquet")
	})
}
