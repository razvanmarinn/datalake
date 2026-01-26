package batcher

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	"github.com/linkedin/goavro/v2"
)

type Schema struct {
	ID            int    `json:"id"`
	Project       string `json:"project_name"`
	Name          string `json:"name"`
	Version       int    `json:"version"`
	AvroSchema    string `json:"avro_schema"`
	ParquetSchema string `json:"parquet_schema"`
}

type MessageBatch struct {
	UUID        uuid.UUID
	Topic       string
	Messages    []Message
	OwnerId     string
	ProjectName string
}

type Message struct {
	Key         []byte
	Value       []byte
	OwnerId     string
	ProjectName string
	Data        map[string]interface{}
}

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

func (b *Batcher) AddMessage(key, value []byte, ownerId, projectId string, data map[string]interface{}) bool {
	b.Current.AddMessage(key, value, ownerId, projectId, data)
	return b.Current.Size() >= b.MaxMessages
}

func (b *Batcher) Flush() *MessageBatch {
	if b.Current.Size() == 0 {
		return nil
	}

	completedBatch := b.Current
	b.Current = NewMessageBatch(b.Topic)

	fmt.Printf("Batch flushed. Size: %d, UUID: %s\n", completedBatch.Size(), completedBatch.UUID)
	return completedBatch
}

func NewMessageBatch(topic string) *MessageBatch {
	return &MessageBatch{
		UUID:     uuid.New(),
		Topic:    topic,
		Messages: make([]Message, 0),
	}
}

func (mb *MessageBatch) AddMessage(key, value []byte, ownerId, projectName string, data map[string]interface{}) {
	mb.Messages = append(mb.Messages, Message{
		Key:         key,
		Value:       value,
		OwnerId:     ownerId,
		ProjectName: projectName,
		Data:        data,
	})

	if len(mb.Messages) == 1 {
		mb.OwnerId = ownerId
		mb.ProjectName = projectName
	}
}

func (mb *MessageBatch) Size() int {
	return len(mb.Messages)
}
func (mb *MessageBatch) GetMessagesAsAvroBytes(schema *Schema) ([]byte, error) {
	if mb.Size() == 0 {
		return nil, fmt.Errorf("cannot convert empty batch")
	}

	buf := new(bytes.Buffer)

	ocfw, err := goavro.NewOCFWriter(goavro.OCFConfig{
		W:               buf,
		Schema:          schema.AvroSchema,
		CompressionName: "snappy",
	})
	if err != nil {
		return nil, fmt.Errorf("OCF writer creation failed: %v", err)
	}

	var records []interface{}
	for _, msg := range mb.Messages {
		cleanData := normalizeValue(msg.Data)
		records = append(records, cleanData)
	}

	if err := ocfw.Append(records); err != nil {
		return nil, fmt.Errorf("avro append failed: %v", err)
	}

	return buf.Bytes(), nil
}

func normalizeValue(v interface{}) interface{} {
	switch v := v.(type) {
	case json.Number:
		if i, err := v.Int64(); err == nil {
			return i
		}
		if f, err := v.Float64(); err == nil {
			return f
		}
		return v.String()
	case map[string]interface{}:
		newMap := make(map[string]interface{}, len(v))
		for k, val := range v {
			newMap[k] = normalizeValue(val)
		}
		return newMap
	case []interface{}:
		newSlice := make([]interface{}, len(v))
		for i, val := range v {
			newSlice[i] = normalizeValue(val)
		}
		return newSlice
	}
	return v
}

func UnmarshalMessage(value []byte) (map[string]interface{}, error) {
	decoder := json.NewDecoder(bytes.NewReader(value))
	decoder.UseNumber()

	var wrapper map[string]interface{}
	if err := decoder.Decode(&wrapper); err != nil {
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

	return dataMap, nil
}
