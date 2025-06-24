package nodes

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"
)

const workerStateFile = "/data/worker_node_state.json"
const masterStateFile = "/data/master_node_state.json"

// WorkerNodeState represents the state of a worker node
type WorkerNodeState struct {
	ID              string   `json:"id"`
	ReceivedBatches []string `json:"received_batches"`
	lock            sync.Mutex
}

type WorkerNodeSerializable struct {
	ID              string   `json:"id"`
	ReceivedBatches []string `json:"received_batches"`
}

type MasterNodeState struct {
	ID    string         `json:"id"`
	Files []FileMetadata `json:"files"`
}

// NewWorkerNodeState initializes a new WorkerNodeState
func NewWorkerNodeState() *WorkerNodeState {
	return &WorkerNodeState{
		ReceivedBatches: make([]string, 0),
	}
}

func NewMasterNodeState() *MasterNodeState {
	return &MasterNodeState{
		Files: make([]FileMetadata, 0),
	}
}

func (w *WorkerNodeState) GetState() ([]byte, error) {
	w.lock.Lock()
	defer w.lock.Unlock()

	data, err := json.Marshal(w)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (w *WorkerNodeState) LoadState(data []byte) error {
	var tempState WorkerNodeSerializable

	if err := json.Unmarshal(data, &tempState); err != nil {
		log.Printf("Error unmarshaling data: %v\nData: %s", err, string(data))
		return err
	}

	w.ID = tempState.ID
	w.ReceivedBatches = tempState.ReceivedBatches

	return nil
}

func (w *WorkerNodeState) UpdateState(workerNode *WorkerNode) (any, error) {
	w.lock.Lock()
	defer w.lock.Unlock()

	serializable := WorkerNodeSerializable{
		ID:              workerNode.ID,
		ReceivedBatches: make([]string, 0, len(workerNode.ReceivedBatches)),
	}

	for key := range workerNode.ReceivedBatches {
		serializable.ReceivedBatches = append(serializable.ReceivedBatches, key)
	}

	data, err := json.Marshal(serializable)
	if err != nil {
		fmt.Printf("Error marshaling WorkerNode: %v\n", err)
		return nil, err
	}

	if err := w.LoadState(data); err != nil {
		fmt.Printf("Error loading state: %v\n", err)
		return nil, err
	}

	return data, nil
}

func (w *WorkerNodeState) SaveState() error {
	data, err := w.GetState()
	if err != nil {
		return err
	}

	file, err := os.Create(workerStateFile)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.Write(data)
	if err != nil {
		return err
	}

	return nil
}

func (w *WorkerNodeState) LoadStateFromFile() error {
	file, err := os.Open(workerStateFile)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		return err
	}

	data := make([]byte, stat.Size())
	_, err = file.Read(data)
	if err != nil {
		return err
	}

	return w.LoadState(data)
}

func (w *WorkerNodeState) SetID(id string) {
	w.lock.Lock()
	defer w.lock.Unlock()
	w.ID = id
}

func (w *WorkerNodeState) getBytesFromPaths() map[string][]byte {
	batches := make(map[string][]byte)
	for _, batch_id := range w.ReceivedBatches {
		file_path := fmt.Sprintf("./batches/%s.bin", batch_id)
		data, err := os.ReadFile(file_path)
		if err != nil {
			log.Printf("Error reading file %s: %v", file_path, err)
			continue
		}
		batches[batch_id] = data
	}
	return batches
}

func (m *MasterNodeState) LoadState(data []byte) error {

	if err := json.Unmarshal(data, m); err != nil {
		log.Printf("Error unmarshaling data: %v\nData: %s", err, string(data))
		return err
	}
	return nil
}

func (m *MasterNodeState) UpdateState(masterNode *MasterNode) {
	m.ID = masterNode.ID
	m.Files = masterNode.FileRegistry
}

func (m *MasterNodeState) GetState() ([]byte, error) {
	data, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (w *MasterNodeState) SaveState() error {
	data, err := w.GetState()
	if err != nil {
		return err
	}

	file, err := os.Create(masterStateFile)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.Write(data)
	if err != nil {
		return err
	}

	return nil
}

func (w *MasterNodeState) LoadStateFromFile() error {
	file, err := os.Open(masterStateFile)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		return err
	}

	data := make([]byte, stat.Size())
	_, err = file.Read(data)
	if err != nil {
		return err
	}

	return w.LoadState(data)
}
