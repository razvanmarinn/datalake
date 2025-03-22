package batcher

import "github.com/google/uuid"

type MessageBatch struct {
	Topic   string
	Messages []Message
}

type Message struct {
	UUID  uuid.UUID
	Key   []byte
	Value []byte
}

func NewMessageBatch(topic string) *MessageBatch {
	return &MessageBatch{
		Topic:   topic,
		Messages: make([]Message, 0),
	}
}

func (mb *MessageBatch) AddMessage(key, value []byte) {
	mb.Messages = append(mb.Messages, Message{
		UUID:  uuid.New(),
		Key:   key,
		Value: value,
	})
}

func (mb *MessageBatch) IsFull() bool {
	return len(mb.Messages) >= 10*1024*1024
}

func (mb *MessageBatch) Clean() {
	mb.Messages = make([]Message, 0)
}
