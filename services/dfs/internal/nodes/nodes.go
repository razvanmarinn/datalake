package nodes

import (
    "fmt"
    "log"
    "sync"

    pb "github.com/razvanmarinn/datalake/protobuf"

    "github.com/google/uuid"
    "github.com/razvanmarinn/dfs/internal/load_balancer"
)

var singleInstance *MasterNode
var lock sync.Mutex

type FileFormat string

const (
    FormatBinary  FileFormat = "binary"
    FormatAvro    FileFormat = "avro"
    FormatParquet FileFormat = "parquet"
)

type FileType int

const (
    FileTypeRaw       FileType = 0 
    FileTypeCompacted FileType = 1
)

type Node interface {
    Start()
    Stop()
    HealthCheck() bool
}

type MasterNode struct {
    ID           string
    
    Namespace    map[string]*Inode
    
    BlockMap     map[uuid.UUID]*BlockMetadata

    LoadBalancer *load_balancer.LoadBalancer
    lock         sync.Mutex
}

type Inode struct {
    ID          string      `json:"id"`
    Name        string      `json:"name"`  
    Path        string      `json:"path"` 
    OwnerID     string      `json:"ownerId"`
    ProjectID   string      `json:"projectId"`
    Size        int64       `json:"size"`      
    Format      FileFormat  `json:"format"`   
    Type        FileType    `json:"type"`     
    CreatedAt   int64       `json:"createdAt"`
    Blocks      []uuid.UUID `json:"blocks"` 
}


type BlockMetadata struct {
    BlockID   uuid.UUID   `json:"blockId"`
    Size      int64       `json:"size"`
    
    Replicas  []uuid.UUID `json:"replicas"` 
}

type WorkerNode struct {
    ID              string
    lock            sync.Mutex

    StoredBlocks    map[string][]byte 
    
    pb.UnimplementedDfsServiceServer
}

func NewMasterNode() *MasterNode {
    return &MasterNode{
        ID:        uuid.New().String(),
        Namespace: make(map[string]*Inode),
        BlockMap:  make(map[uuid.UUID]*BlockMetadata),
    }
}

func NewWorkerNode() *WorkerNode {
    return &WorkerNode{
        ID:           uuid.New().String(),
        StoredBlocks: make(map[string][]byte),
    }
}

func NewWorkerNodeWithState(state *WorkerNodeState) *WorkerNode {
    if state.ID == "" {
        return NewWorkerNode()
    }
    return &WorkerNode{
        ID:           state.ID,
        StoredBlocks: state.getBytesFromPaths(), 
    }
}

func NewMasterNodeWithState(state *MasterNodeState) *MasterNode {
    if state.ID == "" {
        return NewMasterNode()
    }

    return &MasterNode{
        ID:        state.ID,
        Namespace: state.Namespace,
        BlockMap:  state.BlockMap,
    }
}

func GetMasterNodeInstance() *MasterNode {
    if singleInstance == nil {
        lock.Lock()
        defer lock.Unlock()
        if singleInstance == nil {
            state := NewMasterNodeState()
            // Ensure LoadStateFromFile populates Namespace and BlockMap correctly
            if err := state.LoadStateFromFile(); err != nil {
                log.Fatalf("Failed to load master node state: %v", err)
            }
            singleInstance = NewMasterNodeWithState(state)
        } else {
            fmt.Println("Single instance already created.")
        }
    } else {
        fmt.Println("Single instance already created.")
    }

    return singleInstance
}