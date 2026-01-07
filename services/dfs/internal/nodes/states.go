package nodes

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"

	"github.com/google/uuid"
)

const (
	workerStateFile = "worker_node_state.json"
	masterStateFile = "master_node_state.json"
)

type WorkerNodeState struct {
	ID           string   `json:"id"`
	StoredBlocks []string `json:"stored_blocks"` // List of Block IDs found on disk
	lock         sync.Mutex
}

func NewWorkerNodeState() *WorkerNodeState {
	return &WorkerNodeState{
		StoredBlocks: make([]string, 0),
	}
}

func (w *WorkerNodeState) GetState() ([]byte, error) {
	w.lock.Lock()
	defer w.lock.Unlock()

	data, err := json.MarshalIndent(w, "", "  ")
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (w *WorkerNodeState) LoadState(data []byte) error {
	w.lock.Lock()
	defer w.lock.Unlock()

	if err := json.Unmarshal(data, w); err != nil {
		log.Printf("Error unmarshaling worker data: %v", err)
		return err
	}
	return nil
}
func (w *WorkerNodeState) UpdateState(workerNode *WorkerNode) error {
	w.lock.Lock()
	defer w.lock.Unlock()

	w.ID = workerNode.ID

	files, err := os.ReadDir(workerNode.StorageDir)
	if err != nil {
		return fmt.Errorf("failed to read storage dir: %v", err)
	}

	blockIDs := make([]string, 0)
	for _, file := range files {
		if !file.IsDir() && strings.HasSuffix(file.Name(), ".bin") {
			// filename is "uuid.bin", remove ".bin"
			blockID := strings.TrimSuffix(file.Name(), ".bin")
			blockIDs = append(blockIDs, blockID)
		}
	}

	w.StoredBlocks = blockIDs
	return nil
}

func (w *WorkerNodeState) SaveState(pathOverride ...string) error {
	data, err := w.GetState()
	if err != nil {
		return err
	}

	path := workerStateFile
	if len(pathOverride) > 0 {
		path = pathOverride[0]
	}

	tmpPath := path + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return err
	}

	return os.Rename(tmpPath, path)
}

func (w *WorkerNodeState) LoadStateFromFile(pathOverride ...string) error {
	path := workerStateFile
	if len(pathOverride) > 0 {
		path = pathOverride[0]
	}

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	return w.LoadState(data)
}

type MasterNodeState struct {
	ID        string                       `json:"id"`
	Namespace map[string]*Inode            `json:"namespace"` // Map of Path -> Inode
	BlockMap  map[uuid.UUID]*BlockMetadata `json:"block_map"` // Map of BlockUUID -> BlockMetadata
}

func NewMasterNodeState() *MasterNodeState {
	return &MasterNodeState{
		Namespace: make(map[string]*Inode),
		BlockMap:  make(map[uuid.UUID]*BlockMetadata),
	}
}

func (m *MasterNodeState) LoadState(data []byte) error {
	if err := json.Unmarshal(data, m); err != nil {
		log.Printf("Error unmarshaling master data: %v", err)
		return err
	}

	if m.Namespace == nil {
		m.Namespace = make(map[string]*Inode)
	}
	if m.BlockMap == nil {
		m.BlockMap = make(map[uuid.UUID]*BlockMetadata)
	}
	return nil
}

func (m *MasterNodeState) UpdateState(masterNode *MasterNode) {
	masterNode.lock.Lock()
	defer masterNode.lock.Unlock()

	m.ID = masterNode.ID
	m.Namespace = masterNode.Namespace
	m.BlockMap = masterNode.BlockMap
}

func (m *MasterNodeState) GetState() ([]byte, error) {
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (m *MasterNodeState) SaveState() error {
	data, err := m.GetState()
	if err != nil {
		return err
	}

	tmpPath := masterStateFile + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return err
	}

	return os.Rename(tmpPath, masterStateFile)
}

func (m *MasterNodeState) LoadStateFromFile() error {
	data, err := os.ReadFile(masterStateFile)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	return m.LoadState(data)
}
