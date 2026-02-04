package helpers

import (
	"encoding/json"
	"testing"

	"github.com/razvanmarinn/metadata-service/internal/models"
	"github.com/stretchr/testify/assert"
)

func TestGenerateAvroSchema(t *testing.T) {
	fields := []models.Field{
		{Name: "id", Type: "int"},
		{Name: "name", Type: "string"},
	}

	schema, err := generateAvroSchema("TestRecord", "org.test", fields)
	assert.NoError(t, err)
	assert.Contains(t, schema, "record")
	assert.Contains(t, schema, "TestRecord")
	assert.Contains(t, schema, "org.test")

	var parsed map[string]interface{}
	err = json.Unmarshal([]byte(schema), &parsed)
	assert.NoError(t, err)
	assert.Equal(t, "record", parsed["type"])
	assert.Equal(t, "TestRecord", parsed["name"])
}

func TestCreateSchemaBody_Validation(t *testing.T) {
	t.Run("valid body", func(t *testing.T) {
		body := CreateSchemaBody{
			SchemaName: "test-schema",
			Type:       "record",
			AvroFields: []models.Field{{Name: "id", Type: "int"}},
			ParquetDef: "parquet-definition",
		}
		assert.NotEmpty(t, body.SchemaName)
		assert.NotEmpty(t, body.Type)
		assert.Len(t, body.AvroFields, 1)
	})

	t.Run("empty schema name", func(t *testing.T) {
		body := CreateSchemaBody{
			SchemaName: "",
		}
		assert.Empty(t, body.SchemaName)
	})
}

func TestAvroSchemaFormat(t *testing.T) {
	t.Run("generates valid JSON", func(t *testing.T) {
		fields := []models.Field{
			{Name: "user_id", Type: "long"},
			{Name: "email", Type: "string"},
			{Name: "active", Type: "boolean"},
		}

		schema, err := generateAvroSchema("User", "com.example", fields)
		assert.NoError(t, err)

		var parsed map[string]interface{}
		err = json.Unmarshal([]byte(schema), &parsed)
		assert.NoError(t, err)

		fieldsData := parsed["fields"].([]interface{})
		assert.Len(t, fieldsData, 3)
	})

	t.Run("handles empty fields", func(t *testing.T) {
		schema, err := generateAvroSchema("Empty", "com.test", []models.Field{})
		assert.NoError(t, err)
		assert.Contains(t, schema, "fields")
	})
}
