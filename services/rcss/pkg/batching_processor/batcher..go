package batchingprocessor

import "github.com/razvanmarinn/rcss/pkg/batches"

const BATCH_SIZE = (1024 * 1024) * 62

type BatchProcessor struct {
	data []byte
}

func NewBatchProcessor(data []byte) *BatchProcessor {
	return &BatchProcessor{
		data: data,
	}
}

func (bp *BatchProcessor) Process() []batches.Batch {
	var _batches []batches.Batch
	dataLen := len(bp.data)
	for i := 0; i < dataLen; i += BATCH_SIZE {
		end := i + BATCH_SIZE
		if end > dataLen {
			end = dataLen
		}
		batchData := make([]byte, end-i)
		copy(batchData, bp.data[i:end])
		batch := batches.NewBatch(batchData)
		_batches = append(_batches, batch)
	}
	return _batches
}
