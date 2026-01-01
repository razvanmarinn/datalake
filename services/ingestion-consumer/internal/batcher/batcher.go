package batcher

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/linkedin/goavro/v2"
)

// --- Structs ---

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
	UUID      uuid.UUID
	Topic     string
	Messages  []Message
	OwnerId   string
	ProjectId string
}

type Message struct {
	Key       []byte
	Value     []byte
	OwnerId   string
	ProjectId string
	Data      map[string]interface{}
}

// --- Batcher Logic ---

type Batcher struct {
	Topic       string
	MaxMessages int
	Current     *MessageBatch
}

func NewBatcher(topic string, maxMessages int) *Batcher {
	return &Batcher{
		Topic:       topic,
		MaxMessages: maxMessages,
		Current:     NewMessageBatch(topic),
	}
}

// AddMessage appends to the current batch.
// It returns true if the batch is full and should be flushed.
func (b *Batcher) AddMessage(key, value []byte, ownerId, projectId string, data map[string]interface{}) bool {
	b.Current.AddMessage(key, value, ownerId, projectId, data)
	return b.Current.Size() >= b.MaxMessages
}

// Flush returns the current batch and resets the internal state.
func (b *Batcher) Flush() *MessageBatch {
	if b.Current.Size() == 0 {
		return nil
	}

	completedBatch := b.Current
	b.Current = NewMessageBatch(b.Topic)

	fmt.Printf("Batch flushed. Size: %d, UUID: %s\n", completedBatch.Size(), completedBatch.UUID)
	return completedBatch
}

// --- MessageBatch Logic ---

func NewMessageBatch(topic string) *MessageBatch {
	return &MessageBatch{
		UUID:     uuid.New(),
		Topic:    topic,
		Messages: make([]Message, 0),
	}
}

func (mb *MessageBatch) AddMessage(key, value []byte, ownerId, projectId string, data map[string]interface{}) {
	mb.Messages = append(mb.Messages, Message{
		Key:       key,
		Value:     value,
		OwnerId:   ownerId,
		ProjectId: projectId,
		Data:      data,
	})

	// Set Owner/Project ID on the batch level if it's the first message
	if len(mb.Messages) == 1 {
		mb.OwnerId = ownerId
		mb.ProjectId = projectId
	}
}

func (mb *MessageBatch) Size() int {
	return len(mb.Messages)
}

// --- Avro Conversion Logic ---

func (mb *MessageBatch) GetMessagesAsAvroBytes(schema *Schema) ([]byte, error) {
	if mb.Size() == 0 {
		return nil, fmt.Errorf("cannot convert empty batch")
	}

	buf := new(bytes.Buffer)
	avroSchemaJSON := schemaToAvroSchema(schema)

	ocfw, err := goavro.NewOCFWriter(goavro.OCFConfig{
		W:      buf,
		Schema: avroSchemaJSON,
	})
	if err != nil {
		return nil, fmt.Errorf("OCF writer creation failed: %v", err)
	}

	// Prepare records
	var records []interface{}
	for _, msg := range mb.Messages {
		records = append(records, msg.Data)
	}

	if err := ocfw.Append(records); err != nil {
		return nil, fmt.Errorf("avro append failed: %v", err)
	}

	return buf.Bytes(), nil
}

// --- Helper Functions ---

func UnmarshalMessage(schema *Schema, value []byte) (map[string]interface{}, error) {
	// 1. Unmarshal wrapper (assuming format: { "data": {...}, ... })
	var wrapper map[string]interface{}
	if err := json.Unmarshal(value, &wrapper); err != nil {
		return nil, fmt.Errorf("failed to unmarshal JSON wrapper: %w", err)
	}

	rawData, ok := wrapper["data"]
	if !ok {
		return nil, fmt.Errorf("missing 'data' field in message")
	}

	dataMap, ok := rawData.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("'data' field is not a JSON object")
	}

	// 2. Validate and cast fields against Schema
	result := make(map[string]interface{})
	for _, field := range schema.Fields {
		val, exists := dataMap[field.Name]
		if !exists {
			// Optional: Decide if you want to error out or allow missing fields
			return nil, fmt.Errorf("field '%s' missing", field.Name)
		}

		// Simple Type Casting
		switch field.Type {
		case "int":
			if f, ok := val.(float64); ok { // JSON numbers are floats in Go
				result[field.Name] = int(f)
			} else {
				return nil, fmt.Errorf("field '%s' expected int", field.Name)
			}
		case "string":
			if s, ok := val.(string); ok {
				result[field.Name] = s
			} else {
				return nil, fmt.Errorf("field '%s' expected string", field.Name)
			}
		default:
			// Fallback for other types
			result[field.Name] = val
		}
	}

	return result, nil
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
		fType := "string"
		if field.Type == "int" {
			fType = "int"
		}
		// Escape JSON structure for Avro
		avroFields = append(avroFields, fmt.Sprintf(`{"name": "%s", "type": "%s"}`, field.Name, fType))
	}
	return "[" + strings.Join(avroFields, ",") + "]"
}
