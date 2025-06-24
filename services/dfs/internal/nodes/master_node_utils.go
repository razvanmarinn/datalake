package nodes

import (
	"fmt"
	"log"

	pb "github.com/razvanmarinn/datalake/protobuf"

	"github.com/google/uuid"
	"github.com/razvanmarinn/dfs/internal/load_balancer"
)

func (mn *MasterNode) GetFileBatches(file string) []uuid.UUID {
	mn.lock.Lock()
	defer mn.lock.Unlock()
	for _, fileMeta := range mn.FileRegistry {
		if fileMeta.Name == file {
			return fileMeta.Batches
		}
	}
	return nil
}

func (mn *MasterNode) GetBatchLocations(batchUUID uuid.UUID) []uuid.UUID {
	mn.lock.Lock()
	defer mn.lock.Unlock()
	for _, fileMeta := range mn.FileRegistry {
		if locations, exists := fileMeta.BatchLocations[batchUUID]; exists {
			return locations
		}
	}
	return nil
}

func (mn *MasterNode) Start() {
	fmt.Println("MasterNode started")

}

func (mn *MasterNode) Stop() {
}

func (mn *MasterNode) HealthCheck() bool {
	return true
}

func (mn *MasterNode) UpdateBatchLocation(batchUUID uuid.UUID, workerNodeID string) {
	mn.lock.Lock()
	defer mn.lock.Unlock()
	for i, fileMeta := range mn.FileRegistry {
		if _, exists := fileMeta.BatchLocations[batchUUID]; exists {
			workerNodeUUID, err := uuid.Parse(workerNodeID)
			if err != nil {
				fmt.Printf("Error parsing worker node ID %s: %v\n", workerNodeID, err)
				return
			}
			mn.FileRegistry[i].BatchLocations[batchUUID] = append(mn.FileRegistry[i].BatchLocations[batchUUID], workerNodeUUID)
		}
	}

	fmt.Printf("Batch %s location updated to %s\n", batchUUID, workerNodeID)
}

func (m *MasterNode) GetWorkerAllocatedForBatch(batchID uuid.UUID) (string, error) {

	for _, fileMeta := range m.FileRegistry {
		if workers, exists := fileMeta.BatchLocations[batchID]; exists {
			if len(workers) == 0 {
				return "", fmt.Errorf("no workers allocated for batch %s", batchID)
			}
			workerID := workers[0]
			return workerID.String(), nil
		}
	}
	return "", fmt.Errorf("batch %s not found", batchID)
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

func (mn *MasterNode) RegisterFile(in *pb.ClientFileRequestToMaster) *FileMetadata {
	log.Printf("Registering file %s\n", in.GetFileName())
	log.Printf("File size: %d\n", in.GetFileSize())
	log.Printf("File format %s\n", in.GetFileFormat())

	fMetadata := &FileMetadata{
		Name:           in.GetFileName(),
		OwnerID:        in.GetOwnerId(),
		ProjectID:      in.GetProjectId(),
		Size:           in.GetFileSize(),
		Hash:           uint32(in.GetHash()),
		Format:         FileFormat(in.GetFileFormat()),
		Batches:        make([]uuid.UUID, len(in.BatchInfo.Batches)),
		BatchSizes:     make(map[uuid.UUID]int),
		BatchLocations: make(map[uuid.UUID][]uuid.UUID),
	}

	for i, batch := range in.BatchInfo.Batches {

		batchUUID, err := uuid.Parse(batch.Uuid)
		if err != nil {
			fmt.Printf("Error parsing batch UUID: %v\n", err)
			continue
		}

		fMetadata.Batches[i] = batchUUID
		fMetadata.BatchSizes[batchUUID] = int(batch.Size)

		fMetadata.BatchLocations[batchUUID] = make([]uuid.UUID, 0)
	}
	log.Printf("File %s has been registered\n", fMetadata.Name)
	return fMetadata
}
