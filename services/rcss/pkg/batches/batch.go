package batches

import (
	"github.com/google/uuid"
)

type Batch struct {
	UUID uuid.UUID
	Data []byte
}

func NewBatch(data []byte) Batch {
	return Batch{
		UUID: uuid.New(),
		Data: data,
	}
}
