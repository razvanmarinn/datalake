package batcher

import "github.com/google/uuid"

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
