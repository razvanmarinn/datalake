package nodes

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	commonv1 "github.com/razvanmarinn/datalake/protobuf/gen/go/common/v1"
	coordinatorv1 "github.com/razvanmarinn/datalake/protobuf/gen/go/coordinator/v1"

	"github.com/google/uuid"
	"github.com/razvanmarinn/dfs/internal/load_balancer"
)

var singleInstance *MasterNode

type OpType int

const (
	OpRegisterFile OpType = iota
	OpDeleteFile
	OpRegisterDir
	OpRenameFile
)

type OperationLogEntry struct {
	OpType    OpType      `json:"opType"`
	Timestamp int64       `json:"timestamp"`
	Payload   interface{} `json:"payload"` // Could be Inode, or DeleteRequest
}

type MasterNode struct {
	ID string

	Namespace map[string]*Inode // PROJECT -> INODEs

	BlockMap     map[uuid.UUID]*BlockMetadata // INOTE UUID - > Blocks
	opLogFile    *os.File
	opLock       sync.Mutex
	LoadBalancer *load_balancer.LoadBalancer
	lock         sync.Mutex
}

func (mn *MasterNode) appendToLog(op OperationLogEntry) error {
	mn.opLock.Lock()
	defer mn.opLock.Unlock()

	data, _ := json.Marshal(op)

	if _, err := mn.opLogFile.Write(append(data, '\n')); err != nil {
		return err
	}
	return mn.opLogFile.Sync()
}

func NewMasterNode() *MasterNode {
	return &MasterNode{
		ID:        uuid.New().String(),
		Namespace: make(map[string]*Inode),
		BlockMap:  make(map[uuid.UUID]*BlockMetadata),
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

func (mn *MasterNode) GetFileBatches(filePath string) []uuid.UUID {
	mn.lock.Lock()
	defer mn.lock.Unlock()

	if inode, exists := mn.Namespace[filePath]; exists {
		return inode.Blocks
	}
	return nil
}

// GetBatchLocations retrieves the list of worker nodes holding a specific block.
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

func (mn *MasterNode) UpdateBlockLocation(blockUUID uuid.UUID, workerNodeID string) {
	mn.lock.Lock()
	defer mn.lock.Unlock()

	blockMeta, exists := mn.BlockMap[blockUUID]
	if !exists {
		fmt.Printf("Warning: received update for unknown block %s\n", blockUUID)
		return
	}

	workerUUID, err := uuid.Parse(workerNodeID)
	if err != nil {
		fmt.Printf("Error parsing worker node ID %s: %v\n", workerNodeID, err)
		return
	}

	for _, replica := range blockMeta.Replicas {
		if replica == workerUUID {
			return
		}
	}

	blockMeta.Replicas = append(blockMeta.Replicas, workerUUID)
	fmt.Printf("Block %s location updated to %s\n", blockUUID, workerNodeID)
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
func (mn *MasterNode) ensureDirectory(fullPath, name, ownerID, projectID string) {
	if _, exists := mn.Namespace[fullPath]; exists {
		return
	}

	dirInode := &Inode{
		ID:        uuid.New().String(),
		Name:      name,
		Path:      fullPath,
		Type:      DirType,
		ProjectID: projectID,
		Size:      0,
		Blocks:    nil,
		Children:  make([]string, 0),
	}

	op := OperationLogEntry{
		OpType:    OpRegisterDir, // You might need to add this OpType
		Timestamp: time.Now().Unix(),
		Payload:   dirInode,
	}
	_ = mn.appendToLog(op) // Handle error in prod

	mn.Namespace[fullPath] = dirInode
	log.Printf("Created directory: %s", fullPath)
}

func (mn *MasterNode) AllocateBlock(req *coordinatorv1.AllocateBlockRequest) (*coordinatorv1.AllocateBlockResponse, error) {
	mn.lock.Lock()
	defer mn.lock.Unlock()

	newBlockID := uuid.New()

	workerID, workerMeta := mn.LoadBalancer.GetNextClient()

	fullAddress := fmt.Sprintf("%s:%d", workerMeta.Ip, workerMeta.Port)

	targetNodes := []*commonv1.BlockLocation{
		{
			BlockId:  newBlockID.String(),
			WorkerId: workerID,
			Address:  fullAddress,
		},
	}

	mn.BlockMap[newBlockID] = &BlockMetadata{
		BlockID:  newBlockID,
		Size:     req.SizeBytes,
		Replicas: []uuid.UUID{}, // Will be filled when the DataNode confirms receipt or Client commits
	}

	return &coordinatorv1.AllocateBlockResponse{
		BlockId:         newBlockID.String(),
		TargetDatanodes: targetNodes,
	}, nil
}

func (mn *MasterNode) CommitFile(req *coordinatorv1.CommitFileRequest) (*Inode, error) {
	mn.lock.Lock()
	defer mn.lock.Unlock()

	if req.ProjectId == "" || req.FilePath == "" {
		return nil, fmt.Errorf("invalid project_id or file_path")
	}

	// Path Logic
	fullPath := filepath.Join(req.ProjectId, req.FilePath)
	dirPath := filepath.Dir(fullPath)

	// Ensure directories exist
	mn.ensureDirectory(req.ProjectId, req.ProjectId, "system", req.ProjectId)
	mn.ensureDirectory(dirPath, filepath.Base(dirPath), "system", req.ProjectId)

	var totalSize int64
	blockUUIDs := make([]uuid.UUID, 0, len(req.Blocks))

	for _, b := range req.Blocks {
		// Parse UUID from the string in BlockInfo
		bid, err := uuid.Parse(b.BlockId)
		if err != nil {
			return nil, fmt.Errorf("invalid block uuid %s: %v", b.BlockId, err)
		}

		blockUUIDs = append(blockUUIDs, bid)
		totalSize += b.Size

		// Update/Verify Block Metadata
		if _, exists := mn.BlockMap[bid]; !exists {
			mn.BlockMap[bid] = &BlockMetadata{
				BlockID:  bid,
				Size:     b.Size,
				Replicas: make([]uuid.UUID, 0),
			}
		}
	}

	// Create Inode
	inode := &Inode{
		ID:        uuid.New().String(),
		Name:      filepath.Base(fullPath),
		Path:      fullPath,
		Type:      FileType,
		ProjectID: req.ProjectId,
		OwnerID:   req.OwnerId,
		Size:      totalSize,
		Blocks:    blockUUIDs,
	}

	// Log Operation
	op := OperationLogEntry{
		OpType:    OpRegisterFile,
		Timestamp: time.Now().Unix(),
		Payload:   inode,
	}
	if err := mn.appendToLog(op); err != nil {
		return nil, err
	}

	// Update Namespace
	mn.Namespace[inode.Path] = inode
	if parent, ok := mn.Namespace[dirPath]; ok {
		parent.Children = append(parent.Children, inode.ID)
	}

	return inode, nil
}

func (mn *MasterNode) GetFileMetadata(projectID, filePath string) (*coordinatorv1.GetFileMetadataResponse, error) {
	mn.lock.Lock()
	defer mn.lock.Unlock()

	fullPath := filepath.Join(projectID, filePath)
	inode, exists := mn.Namespace[fullPath]
	if !exists {
		return nil, fmt.Errorf("file not found: %s", fullPath)
	}

	blocks := make([]*commonv1.BlockInfo, 0)
	locations := make(map[string]*commonv1.BlockLocation)

	for _, blockUUID := range inode.Blocks {
		blockMeta, metaExists := mn.BlockMap[blockUUID]
		if !metaExists {
			continue
		}

		blocks = append(blocks, &commonv1.BlockInfo{
			BlockId:  blockUUID.String(),
			Size:     blockMeta.Size,
			Checksum: 0, // Calculate or store checksums if needed
		})

		if len(blockMeta.Replicas) > 0 {
			workerUUID := blockMeta.Replicas[0] // Just taking the first replica for now

			_, workerInfo, _, _, err := mn.LoadBalancer.GetClientByWorkerID(workerUUID.String())

			if err == nil {
				fullAddress := fmt.Sprintf("%s:%d", workerInfo.Ip, workerInfo.Port)

				locations[blockUUID.String()] = &commonv1.BlockLocation{
					BlockId:  blockUUID.String(),
					WorkerId: workerUUID.String(),
					Address:  fullAddress,
				}
			}
		}
	}

	return &coordinatorv1.GetFileMetadataResponse{
		Blocks:    blocks,
		Locations: locations,
	}, nil
}

func (mn *MasterNode) ListFiles(projectID, prefix string) []string {
	mn.lock.Lock()
	defer mn.lock.Unlock()

	var files []string
	for _, inode := range mn.Namespace {
		if inode.Type == FileType && inode.ProjectID == projectID {
			relPath, err := filepath.Rel(projectID, inode.Path)
			if err == nil && (prefix == "" || filepath.HasPrefix(relPath, prefix)) {
				files = append(files, relPath)
			}
		}
	}
	return files
}
