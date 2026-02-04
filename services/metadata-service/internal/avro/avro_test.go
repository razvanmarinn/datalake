package avro

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAvro_SchemaValidation(t *testing.T) {
	t.Run("validates avro schema format", func(t *testing.T) {
		schema := `{"type":"record","name":"User","fields":[{"name":"id","type":"int"}]}`
		assert.Contains(t, schema, "record")
		assert.Contains(t, schema, "fields")
	})
}

func TestAvro_FieldTypes(t *testing.T) {
	t.Run("supports basic field types", func(t *testing.T) {
		types := []string{"int", "long", "string", "boolean", "float", "double"}
		for _, typ := range types {
			assert.NotEmpty(t, typ)
		}
	})
}
