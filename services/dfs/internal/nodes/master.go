package nodes

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	commonv1 "github.com/razvanmarinn/datalake/protobuf/gen/go/common/v1"
	coordinatorv1 "github.com/razvanmarinn/datalake/protobuf/gen/go/coordinator/v1"
	datanodev1 "github.com/razvanmarinn/datalake/protobuf/gen/go/datanode/v1"

	"github.com/google/uuid"
	"github.com/razvanmarinn/dfs/internal/load_balancer"
)

var singleInstance *MasterNode

const storageDir = "/data"

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
	Payload   interface{} `json:"payload"`
}

type MasterNode struct {
	ID string

	Namespace map[string]*Inode

	BlockMap     map[uuid.UUID]*BlockMetadata
	opLogFile    *os.File
	opLock       sync.Mutex
	LoadBalancer *load_balancer.LoadBalancer
	lock         sync.Mutex
	IsActive     bool
	Replicator   *Replicator
}

func (mn *MasterNode) appendToLog(op OperationLogEntry) error {
	mn.opLock.Lock()
	defer mn.opLock.Unlock()

	data, _ := json.Marshal(op)

	if _, err := mn.opLogFile.Write(append(data, '\n')); err != nil {
		return err
	}
	mn.opLogFile.Sync()
	if mn.IsActive && mn.Replicator != nil {
		if err := mn.Replicator.SendToQuorum(context.Background(), op); err != nil {
			log.Fatalf("Critical: Lost Quorum during write: %v", err)
		}
	}

	return nil
}

func NewMasterNode() *MasterNode {
	logPath := filepath.Join(storageDir, "master_op.log")

	if err := os.MkdirAll(storageDir, 0755); err != nil {
		log.Printf("Warning: could not create storage dir: %v", err)
	}

	f, err := os.OpenFile(logPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("Failed to open operation log at %s: %v", logPath, err)
	}

	return &MasterNode{
		ID:        uuid.New().String(),
		Namespace: make(map[string]*Inode),
		BlockMap:  make(map[uuid.UUID]*BlockMetadata),
		opLogFile: f,
	}
}

func (mn *MasterNode) ApplyReplicatedLog(opType OpType, payload []byte) error {
	mn.lock.Lock()
	defer mn.lock.Unlock()

	if opType == OpRegisterFile {
		var inode Inode
		json.Unmarshal(payload, &inode)
		mn.Namespace[inode.Path] = &inode
	}

	op := OperationLogEntry{OpType: opType, Payload: payload /* logic needed */}
	mn.appendToLog(op)

	return nil
}

func NewMasterNodeWithState(state *MasterNodeState) *MasterNode {
	if state.ID == "" {
		return NewMasterNode()
	}

	logPath := filepath.Join(storageDir, "master_op.log")

	f, err := os.OpenFile(logPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("Failed to open operation log at %s: %v", logPath, err)
	}

	return &MasterNode{
		ID:        state.ID,
		Namespace: state.Namespace,
		BlockMap:  state.BlockMap,
		opLogFile: f,
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
		OpType:    OpRegisterDir,
		Timestamp: time.Now().Unix(),
		Payload:   dirInode,
	}
	_ = mn.appendToLog(op)

	mn.Namespace[fullPath] = dirInode
	log.Printf("Created directory: %s", fullPath)
}

func (mn *MasterNode) AllocateBlock(req *coordinatorv1.AllocateBlockRequest) (*coordinatorv1.AllocateBlockResponse, error) {
	mn.lock.Lock()
	defer mn.lock.Unlock()

	newBlockID := uuid.New()

	workerID, workerMeta := mn.LoadBalancer.GetNextClient()

	workerUUID, err := uuid.Parse(workerID)
	if err != nil {
		return nil, fmt.Errorf("failed to parse worker uuid: %v", err)
	}

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
		Replicas: []uuid.UUID{workerUUID},
	}

	log.Printf("Allocated block %s to worker %s", newBlockID, workerID)

	return &coordinatorv1.AllocateBlockResponse{
		BlockId:         newBlockID.String(),
		TargetDatanodes: targetNodes,
	}, nil
}
func (mn *MasterNode) commitFileInternal(req *coordinatorv1.CommitFileRequest) (*Inode, error) {
	if req.ProjectId == "" || req.FilePath == "" {
		return nil, fmt.Errorf("invalid project_id or file_path")
	}
	fullPath := filepath.Clean(req.FilePath)
	dirPath := filepath.Dir(fullPath)

	parts := strings.Split(fullPath, string(filepath.Separator))
	if len(parts) > 0 {
		rootDir := parts[0]
		mn.ensureDirectory(rootDir, rootDir, "system", req.ProjectId)
	}

	if len(parts) > 2 {
		mn.ensureDirectory(dirPath, filepath.Base(dirPath), "system", req.ProjectId)
	}

	var totalSize int64
	blockUUIDs := make([]uuid.UUID, 0, len(req.Blocks))

	for _, b := range req.Blocks {
		bid, err := uuid.Parse(b.BlockId)
		if err != nil {
			return nil, fmt.Errorf("invalid block uuid %s: %v", b.BlockId, err)
		}

		blockUUIDs = append(blockUUIDs, bid)
		totalSize += b.Size

		if _, exists := mn.BlockMap[bid]; !exists {
			mn.BlockMap[bid] = &BlockMetadata{
				BlockID:  bid,
				Size:     b.Size,
				Replicas: make([]uuid.UUID, 0),
			}
		}
	}

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

	op := OperationLogEntry{
		OpType:    OpRegisterFile,
		Timestamp: time.Now().Unix(),
		Payload:   inode,
	}
	if err := mn.appendToLog(op); err != nil {
		return nil, fmt.Errorf("failed to write operation log: %w", err)
	}

	mn.Namespace[inode.Path] = inode

	if parent, ok := mn.Namespace[dirPath]; ok {
		parent.Children = append(parent.Children, inode.ID)
	} else {
		log.Printf("Warning: Parent directory %s not found for file %s", dirPath, inode.Path)
	}

	return inode, nil
}

func (mn *MasterNode) CommitFile(req *coordinatorv1.CommitFileRequest) (*Inode, error) {
	mn.lock.Lock()
	defer mn.lock.Unlock()
	return mn.commitFileInternal(req)
}

func (mn *MasterNode) CommitCompaction(req *coordinatorv1.CommitCompactionRequest) error {
	mn.lock.Lock()
	defer mn.lock.Unlock()

	log.Printf("Starting Atomic Swap for Compaction. New File: %s", req.NewFile.FilePath)

	_, err := mn.commitFileInternal(req.NewFile)
	if err != nil {
		return fmt.Errorf("failed to register new compacted file: %w", err)
	}

	for _, oldPath := range req.OldFilePaths {
		path := filepath.Clean(oldPath)

		inode, exists := mn.Namespace[path]
		if !exists {
			log.Printf("Warning: Compaction tried to delete non-existent file: %s", path)
			continue
		}

		for _, blockID := range inode.Blocks {
			blockMeta, ok := mn.BlockMap[blockID]
			if !ok {
				continue
			}

			for _, replicaWorkerID := range blockMeta.Replicas {
				client, _, _, _, err := mn.LoadBalancer.GetClientByWorkerID(replicaWorkerID.String())
				if err != nil {
					log.Printf("Error getting client for worker %s to delete block %s: %v", replicaWorkerID, blockID, err)
					continue
				}

				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				_, err = client.DeleteBlock(ctx, &datanodev1.DeleteBlockRequest{
					BlockId: blockID.String(),
				})
				cancel()

				if err != nil {
					log.Printf("Failed to delete block %s on worker %s: %v", blockID, replicaWorkerID, err)
				} else {
					log.Printf("Physically deleted block %s on worker %s", blockID, replicaWorkerID)
				}
			}

			delete(mn.BlockMap, blockID)
		}

		delete(mn.Namespace, path)

		dirPath := filepath.Dir(path)
		if parent, ok := mn.Namespace[dirPath]; ok {
			newChildren := make([]string, 0)
			for _, childID := range parent.Children {
				if childID != inode.ID {
					newChildren = append(newChildren, childID)
				}
			}
			parent.Children = newChildren
		}

		op := OperationLogEntry{
			OpType:    OpDeleteFile,
			Timestamp: time.Now().Unix(),
			Payload:   inode,
		}
		if err := mn.appendToLog(op); err != nil {
			log.Printf("Error logging deletion for %s: %v", path, err)
		}
	}

	log.Printf("Compaction Swap Complete. Removed %d files.", len(req.OldFilePaths))
	return nil
}

func (mn *MasterNode) GetFileMetadata(projectID, filePath string) (*coordinatorv1.GetFileMetadataResponse, error) {
	mn.lock.Lock()
	defer mn.lock.Unlock()

	fullPath := filepath.Clean(filePath)
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
			Checksum: 0,
		})

		if len(blockMeta.Replicas) > 0 {
			workerUUID := blockMeta.Replicas[0]

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

func (mn *MasterNode) ListFiles(projectID, prefix string) ([]string, error) {
	mn.lock.Lock()
	defer mn.lock.Unlock()

	var files []string

	for _, inode := range mn.Namespace {
		if inode.Type == FileType {
			relPath, err := filepath.Rel(projectID, inode.Path)

			if err == nil && !strings.HasPrefix(relPath, "..") {

				if prefix == "" || strings.HasPrefix(relPath, prefix) {
					files = append(files, relPath)
				}
			}
		}
	}
	return files, nil
}
