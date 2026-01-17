package avro

import (
	"encoding/json"

	"github.com/linkedin/goavro/v2"

	"github.com/razvanmarinn/metadata-service/internal/models"
)

func generateAvroSchema(schemaName string, fields []models.Field) (string, error) {
	avroFields := []map[string]interface{}{}
	for _, field := range fields {
		avroFields = append(avroFields, map[string]interface{}{
			"name": field.Name,
			"type": field.Type,
		})
	}

	avroSchema := map[string]interface{}{
		"type":      "record",
		"name":      schemaName,
		"namespace": "com.example",
		"fields":    avroFields,
	}

	schemaBytes, err := json.Marshal(avroSchema)
	if err != nil {
		return "", err
	}

	_, err = goavro.NewCodec(string(schemaBytes))
	if err != nil {
		return "", err
	}

	return string(schemaBytes), nil
}
