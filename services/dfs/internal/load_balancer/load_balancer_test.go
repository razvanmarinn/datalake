package load_balancer

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewWorkerMetadata(t *testing.T) {
	wm := NewWorkerMetadata(nil, "worker-0", 50051, 0)
	assert.NotNil(t, wm)
	assert.Equal(t, "worker-0", wm.Ip)
	assert.Equal(t, int32(50051), wm.Port)
	assert.Equal(t, 0, wm.BatchCount)
}

func TestLoadBalancer_GetNextClient(t *testing.T) {
	lb := &LoadBalancer{
		workerInfo: make(map[string]WorkerMetadata),
		currentIdx: 0,
	}

	wm1 := NewWorkerMetadata(nil, "worker-0", 50051, 0)
	wm2 := NewWorkerMetadata(nil, "worker-1", 50051, 0)
	lb.workerInfo["worker-1"] = *wm1
	lb.workerInfo["worker-2"] = *wm2

	t.Run("returns worker on first call", func(t *testing.T) {
		id, wm := lb.GetNextClient()
		assert.NotEmpty(t, id)
		assert.NotNil(t, wm)
	})

	t.Run("rotates through workers", func(t *testing.T) {
		id1, _ := lb.GetNextClient()
		id2, _ := lb.GetNextClient()
		assert.NotEqual(t, id1, id2)
	})

	t.Run("handles empty worker list", func(t *testing.T) {
		emptyLB := &LoadBalancer{
			workerInfo: make(map[string]WorkerMetadata),
		}
		id, wm := emptyLB.GetNextClient()
		assert.Empty(t, id)
		assert.Equal(t, WorkerMetadata{}, wm)
	})
}

func TestLoadBalancer_Rotate(t *testing.T) {
	lb := &LoadBalancer{
		workerInfo: make(map[string]WorkerMetadata),
	}

	wm := NewWorkerMetadata(nil, "worker-0", 50051, 0)
	lb.workerInfo["worker-1"] = *wm

	id, metadata := lb.Rotate()
	assert.NotEmpty(t, id)
	assert.NotNil(t, metadata)
}

func TestLoadBalancer_GetClientByWorkerID(t *testing.T) {
	lb := &LoadBalancer{
		workerInfo: make(map[string]WorkerMetadata),
	}

	wm := NewWorkerMetadata(nil, "worker-0", 50051, 5)
	lb.workerInfo["test-worker-id"] = *wm

	t.Run("finds existing worker", func(t *testing.T) {
		_, metadata, ip, port, err := lb.GetClientByWorkerID("test-worker-id")
		assert.NoError(t, err)
		assert.Equal(t, "worker-0", ip)
		assert.Equal(t, int32(50051), port)
		assert.Equal(t, 5, metadata.BatchCount)
	})

	t.Run("returns error for non-existent worker", func(t *testing.T) {
		_, _, _, _, err := lb.GetClientByWorkerID("non-existent")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not found")
	})
}

func TestLoadBalancer_Close(t *testing.T) {
	lb := &LoadBalancer{
		workerInfo: make(map[string]WorkerMetadata),
	}

	// Should not panic
	lb.Close()
}

func TestLoadBalancer_ConcurrentAccess(t *testing.T) {
	lb := &LoadBalancer{
		workerInfo: make(map[string]WorkerMetadata),
	}

	for i := 0; i < 5; i++ {
		wm := NewWorkerMetadata(nil, "worker", 50051, 0)
		lb.workerInfo[fmt.Sprintf("worker-%d", i)] = *wm
	}

	done := make(chan bool, 10)
	for i := 0; i < 10; i++ {
		go func() {
			lb.GetNextClient()
			done <- true
		}()
	}

	for i := 0; i < 10; i++ {
		<-done
	}
}
