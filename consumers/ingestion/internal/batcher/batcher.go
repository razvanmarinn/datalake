package batcher

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/linkedin/goavro/v2"
)

type Field struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type Schema struct {
	ID      int     `json:"id"`
	Project string  `json:"project"`
	Name    string  `json:"name"`
	Fields  []Field `json:"fields"`
	Version int     `json:"version"`
}
type MessageBatch struct {
	UUID     uuid.UUID
	Topic    string
	Messages []Message
}

type Message struct {
	Key   []byte
	Value []byte
}

func NewMessageBatch(topic string) *MessageBatch {
	return &MessageBatch{
		UUID:     uuid.New(),
		Topic:    topic,
		Messages: make([]Message, 0),
	}
}

func (mb *MessageBatch) AddMessage(key, value []byte) {
	mb.Messages = append(mb.Messages, Message{
		Key:   key,
		Value: value,
	})
}

func (mb *MessageBatch) Size() int {
	return len(mb.Messages)
}

func (mb *MessageBatch) Clean() {
	mb.Messages = make([]Message, 0)
}

func (mb *MessageBatch) GetMessagesAsBytes() []byte {
	var messagesAsBytes []byte
	for _, message := range mb.Messages {
		messagesAsBytes = append(messagesAsBytes, message.Key...)
		messagesAsBytes = append(messagesAsBytes, message.Value...)
	}
	return messagesAsBytes
}

func schemaToAvroSchema(schema *Schema) string {
	return fmt.Sprintf(`{
        "type": "record",
        "name": "%s",
        "fields": %s
    }`, schema.Name, fieldsToAvroFields(schema.Fields))
}

func fieldsToAvroFields(fields []Field) string {
	var avroFields []string
	for _, field := range fields {
		var fieldType string
		switch field.Type {
		case "int":
			fieldType = "int"
		case "string":
			fieldType = "string"
		default:
			fieldType = "string" // Default to string if unknown
		}
		avroFields = append(avroFields, fmt.Sprintf(`{"name": "%s", "type": "%s"}`, field.Name, fieldType))
	}
	return "[" + strings.Join(avroFields, ",") + "]"
}

func UnmarshalMessage(schema *Schema, value []byte) (map[string]interface{}, error) {
	result := make(map[string]interface{})

	var wrapper map[string]interface{}
	if err := json.Unmarshal(value, &wrapper); err != nil {
		return nil, fmt.Errorf("failed to unmarshal message wrapper: %v", err)
	}

	data, ok := wrapper["data"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("missing or invalid 'data' field in message")
	}

	for _, field := range schema.Fields {
		fieldValue, exists := data[field.Name]
		if !exists {
			return nil, fmt.Errorf("field %s missing in message", field.Name)
		}

		var typedValue interface{}
		switch field.Type {
		case "int":
			floatVal, ok := fieldValue.(float64)
			if !ok {
				return nil, fmt.Errorf("expected numeric value for %s", field.Name)
			}
			typedValue = int(floatVal)
		case "string":
			strVal, ok := fieldValue.(string)
			if !ok {
				return nil, fmt.Errorf("expected string value for %s", field.Name)
			}
			typedValue = strVal
		default:
			return nil, fmt.Errorf("unsupported field type %s", field.Type)
		}

		result[field.Name] = typedValue
	}

	return result, nil
}

func (mb *MessageBatch) GetMessagesAsAvroBytes(schema *Schema) ([]byte, error) {
	buf := new(bytes.Buffer)

	// 1. Create PROPER Avro container writer
	ocfw, err := goavro.NewOCFWriter(goavro.OCFConfig{
		W:      buf,
		Schema: schemaToAvroSchema(schema), // MUST be valid Avro schema
	})
	if err != nil {
		return nil, fmt.Errorf("OCF writer creation failed: %v", err)
	}

	// 2. Convert ALL messages first
	var records []interface{}
	for _, msg := range mb.Messages {
		record, err := UnmarshalMessage(schema, msg.Value)
		if err != nil {
			return nil, fmt.Errorf("message conversion failed: %v", err)
		}
		records = append(records, record)
	}

	// 3. Write in ONE atomic operation
	if err := ocfw.Append(records); err != nil {
		return nil, fmt.Errorf("avro write failed: %v", err)
	}

	// Verify minimum valid file size
	if buf.Len() < 64 {
		return nil, fmt.Errorf("invalid avro output (size %d < 64 bytes)", buf.Len())
	}

	return buf.Bytes(), nil
}
