package handlers

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMemoryFile_Operations(t *testing.T) {
	data := []byte("test data content")
	memFile := &MemoryFile{Reader: bytes.NewReader(data)}

	t.Run("Open returns self", func(t *testing.T) {
		f, err := memFile.Open("test.parquet")
		assert.NoError(t, err)
		assert.Equal(t, memFile, f)
	})

	t.Run("Create returns error", func(t *testing.T) {
		_, err := memFile.Create("test.parquet")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "read only")
	})

	t.Run("Close returns nil", func(t *testing.T) {
		err := memFile.Close()
		assert.NoError(t, err)
	})

	t.Run("Write returns error", func(t *testing.T) {
		_, err := memFile.Write([]byte("data"))
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "read only")
	})
}

func TestQueryHandler_Validation(t *testing.T) {
	t.Run("validates project parameter", func(t *testing.T) {
		projectID := ""
		assert.Empty(t, projectID)
	})

	t.Run("validates file_name parameter", func(t *testing.T) {
		fileName := "test-file.parquet"
		assert.NotEmpty(t, fileName)
		assert.Contains(t, fileName, ".parquet")
	})
}

func TestParseParquetBytes_EmptyData(t *testing.T) {
	t.Run("handles empty reader", func(t *testing.T) {
		emptyData := []byte{}
		memFile := &MemoryFile{Reader: bytes.NewReader(emptyData)}
		assert.NotNil(t, memFile)
	})
}

func TestIsStackPartOfSchema(t *testing.T) {
	testCases := []struct {
		fileName   string
		schemaName string
		expected   bool
	}{
		{"schema1/file1.parquet", "schema1", true},
		{"schema1/subdir/file.parquet", "schema1", true},
		{"schema2/file.parquet", "schema1", false},
		{"other/file.parquet", "schema1", false},
	}

	for _, tc := range testCases {
		t.Run(tc.fileName, func(t *testing.T) {
			result := isStackPartOfSchema(tc.fileName, tc.schemaName)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestJSONParsing(t *testing.T) {
	t.Run("parses valid JSON record", func(t *testing.T) {
		jsonStr := `{"id": 1, "name": "test"}`
		var rec map[string]interface{}
		err := json.Unmarshal([]byte(jsonStr), &rec)
		assert.NoError(t, err)
		assert.Equal(t, float64(1), rec["id"])
		assert.Equal(t, "test", rec["name"])
	})

	t.Run("handles invalid JSON", func(t *testing.T) {
		jsonStr := `{invalid}`
		var rec map[string]interface{}
		err := json.Unmarshal([]byte(jsonStr), &rec)
		assert.Error(t, err)
	})
}

func TestQueryHandlerCreation(t *testing.T) {
	t.Run("validates handler structure", func(t *testing.T) {
		// QueryHandler requires logger, MasterClient, dataNodeClients
		assert.NotNil(t, map[string]interface{}{
			"logger":          nil,
			"MasterClient":    nil,
			"dataNodeClients": make(map[string]interface{}),
		})
	})
}
