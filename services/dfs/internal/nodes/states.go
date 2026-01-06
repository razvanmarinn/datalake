package nodes

import (
    "encoding/json"
    "fmt"
    "log"
    "os"
    "path/filepath"
    "sync"

    "github.com/google/uuid"
)

const (
    workerStateFile = "worker_node_state.json"
    masterStateFile = "master_node_state.json"
    storageRoot     = "/data" 
)


type WorkerNodeState struct {
    ID           string   `json:"id"`
    StoredBlocks []string `json:"stored_blocks"` // Renamed from ReceivedBatches to align with Block architecture
    lock         sync.Mutex
}


type WorkerNodeSerializable struct {
    ID           string   `json:"id"`
    StoredBlocks []string `json:"stored_blocks"`
}

type MasterNodeState struct {
    ID        string                       `json:"id"`
    Namespace map[string]*Inode            `json:"namespace"` // Map of Path -> Inode
    BlockMap  map[uuid.UUID]*BlockMetadata `json:"block_map"` // Map of BlockUUID -> BlockMetadata
}


func NewWorkerNodeState() *WorkerNodeState {
    return &WorkerNodeState{
        StoredBlocks: make([]string, 0),
    }
}

func (w *WorkerNodeState) GetState() ([]byte, error) {
    w.lock.Lock()
    defer w.lock.Unlock()

    serializable := WorkerNodeSerializable{
        ID:           w.ID,
        StoredBlocks: w.StoredBlocks,
    }

    data, err := json.MarshalIndent(serializable, "", "  ")
    if err != nil {
        return nil, err
    }
    return data, nil
}

func (w *WorkerNodeState) LoadState(data []byte) error {
    var tempState WorkerNodeSerializable

    if err := json.Unmarshal(data, &tempState); err != nil {
        log.Printf("Error unmarshaling worker data: %v\nData: %s", err, string(data))
        return err
    }

    w.ID = tempState.ID
    w.StoredBlocks = tempState.StoredBlocks

    return nil
}

func (w *WorkerNodeState) UpdateState(workerNode *WorkerNode) (any, error) {
    w.lock.Lock()
    defer w.lock.Unlock()

    blockIDs := make([]string, 0, len(workerNode.StoredBlocks))
    for key := range workerNode.StoredBlocks {
        blockIDs = append(blockIDs, key)
    }

    w.ID = workerNode.ID
    w.StoredBlocks = blockIDs

    return w.GetState()
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
    return err
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
    blocks := make(map[string][]byte)
    
    for _, blockID := range w.StoredBlocks {
        filePath := filepath.Join(storageRoot, fmt.Sprintf("%s.bin", blockID))
        
        if _, err := os.Stat(filePath); os.IsNotExist(err) {
             localPath := fmt.Sprintf("./%s.bin", blockID)
             if _, err := os.Stat(localPath); err == nil {
                 filePath = localPath
             }
        }

        data, err := os.ReadFile(filePath)
        if err != nil {
            log.Printf("Warning: Could not reload block %s from %s: %v", blockID, filePath, err)
            continue
        }
        blocks[blockID] = data
    }
    return blocks
}


func NewMasterNodeState() *MasterNodeState {
    return &MasterNodeState{
        Namespace: make(map[string]*Inode),
        BlockMap:  make(map[uuid.UUID]*BlockMetadata),
    }
}

func (m *MasterNodeState) LoadState(data []byte) error {
    if err := json.Unmarshal(data, m); err != nil {
        log.Printf("Error unmarshaling master data: %v\nData: %s", err, string(data))
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
    // Acquire lock on the active master node to read safely
    masterNode.lock.Lock()
    defer masterNode.lock.Unlock()

    m.ID = masterNode.ID
    // Direct assignment works because we marshal to JSON afterwards
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

    file, err := os.Create(masterStateFile)
    if err != nil {
        return err
    }
    defer file.Close()

    _, err = file.Write(data)
    return err
}

func (m *MasterNodeState) LoadStateFromFile() error {
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

    return m.LoadState(data)
}