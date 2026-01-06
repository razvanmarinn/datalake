package nodes

import (
    "fmt"
    "log"

    pb "github.com/razvanmarinn/datalake/protobuf"

    "github.com/google/uuid"
    "github.com/razvanmarinn/dfs/internal/load_balancer"
)

// GetFileBatches retrieves the list of Block UUIDs associated with a file.
// (Internally maps File -> Inode -> Blocks)
func (mn *MasterNode) GetFileBatches(filePath string) []uuid.UUID {
    mn.lock.Lock()
    defer mn.lock.Unlock()

    if inode, exists := mn.Namespace[filePath]; exists {
        return inode.Blocks
    }
    return nil
}

// GetBatchLocations retrieves the list of worker nodes holding a specific block.
// (Internally maps BlockUUID -> BlockMetadata -> Replicas)
func (mn *MasterNode) GetBatchLocations(batchUUID uuid.UUID) []uuid.UUID {
    mn.lock.Lock()
    defer mn.lock.Unlock()

    if blockMeta, exists := mn.BlockMap[batchUUID]; exists {
        return blockMeta.Replicas
    }
    return nil
}

func (mn *MasterNode) Start() {
    fmt.Println("MasterNode started")
}

func (mn *MasterNode) Stop() {
    mn.CloseLoadBalancer()
}

func (mn *MasterNode) HealthCheck() bool {
    return true
}

// UpdateBatchLocation records that a specific block is stored on a specific worker.
func (mn *MasterNode) UpdateBatchLocation(batchUUID uuid.UUID, workerNodeID string) {
    mn.lock.Lock()
    defer mn.lock.Unlock()

    blockMeta, exists := mn.BlockMap[batchUUID]
    if !exists {
        fmt.Printf("Warning: received update for unknown block %s\n", batchUUID)
        return
    }

    workerUUID, err := uuid.Parse(workerNodeID)
    if err != nil {
        fmt.Printf("Error parsing worker node ID %s: %v\n", workerNodeID, err)
        return
    }

    // Check if replica already exists to avoid duplicates
    for _, replica := range blockMeta.Replicas {
        if replica == workerUUID {
            return
        }
    }

    blockMeta.Replicas = append(blockMeta.Replicas, workerUUID)
    fmt.Printf("Block %s location updated to %s\n", batchUUID, workerNodeID)
}

func (mn *MasterNode) GetWorkerAllocatedForBatch(batchID uuid.UUID) (string, error) {
    mn.lock.Lock()
    defer mn.lock.Unlock()

    blockMeta, exists := mn.BlockMap[batchID]
    if !exists {
        return "", fmt.Errorf("block %s not found", batchID)
    }

    if len(blockMeta.Replicas) == 0 {
        return "", fmt.Errorf("no workers allocated for block %s", batchID)
    }


    workerID := blockMeta.Replicas[0]
    return workerID.String(), nil
}

func (mn *MasterNode) InitializeLoadBalancer(numWorkers int, basePort int) error {
    lb := load_balancer.NewLoadBalancer(numWorkers, basePort)
    mn.LoadBalancer = lb
    return nil
}

func (mn *MasterNode) CloseLoadBalancer() {
    if mn.LoadBalancer != nil {
        mn.LoadBalancer.Close()
    }
}

// RegisterFile converts the client request (Batches) into the internal DFS structure (Inode + Blocks)
func (mn *MasterNode) RegisterFile(in *pb.ClientFileRequestToMaster) *Inode {
    mn.lock.Lock()
    defer mn.lock.Unlock()

    log.Printf("Registering file %s (Size: %d)\n", in.GetFileName(), in.GetFileSize())

    fileType := FileTypeRaw 

    inode := &Inode{
        ID:        uuid.New().String(),
        Name:      in.GetFileName(),
        Path:      in.GetFileName(), 
        OwnerID:   in.GetOwnerId(),
        ProjectID: in.GetProjectId(),
        Size:      in.GetFileSize(),
        Format:    FileFormat(in.GetFileFormat()),
        Type:      fileType,
        Blocks:    make([]uuid.UUID, len(in.BatchInfo.Batches)),
    }

    for i, batch := range in.BatchInfo.Batches {
        blockUUID, err := uuid.Parse(batch.Uuid)
        if err != nil {
            log.Printf("Error parsing block UUID: %v\n", err)
            continue
        }

        inode.Blocks[i] = blockUUID

        mn.BlockMap[blockUUID] = &BlockMetadata{
            BlockID:  blockUUID,
            Size:     int64(batch.Size),
            Replicas: make([]uuid.UUID, 0),
        }
    }

    mn.Namespace[inode.Path] = inode

    log.Printf("File registered: %s with %d blocks\n", inode.Name, len(inode.Blocks))
    return inode
}

func (mn *MasterNode) Lock() {
	mn.lock.Lock()
}
func (mn *MasterNode) Unlock() {
	mn.lock.Unlock()
}