package nodes

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"

	pb "github.com/razvanmarinn/datalake/protobuf"
)

func (wn *WorkerNode) Start() {
	fmt.Println("WorkerNode started")
}

func (wn *WorkerNode) Stop() {
}

func (wn *WorkerNode) HealthCheck() bool {
	return true
}
func (w *WorkerNode) saveBlock(blockID string, data []byte) error {
	storageDir := "/data"



	filePath := filepath.Join(storageDir, fmt.Sprintf("%s.bin", blockID))

	if err := os.WriteFile(filePath, data, 0644); err != nil {
		return fmt.Errorf("failed to write block file: %w", err)
	}

	return nil
}
func (w *WorkerNode) StoreBlock(ctx context.Context, req *pb.StoreBlockRequest) (*pb.WorkerResponse, error) {

	blockID := req.BlockId

	log.Printf("Received StoreBlock request for block ID: %s, Project: %s, Schema: %s", blockID, req.ProjectId, req.SchemaName)

	w.lock.Lock()

	w.StoredBlocks[blockID] = req.Data

	w.lock.Unlock()

	err := w.saveBlock(blockID, req.Data)

	if err != nil {

		log.Printf("Failed to save block %s: %v", blockID, err)

		return &pb.WorkerResponse{Success: false}, err

	}

	log.Printf("Successfully stored block ID: %s, Data length: %d", blockID, len(req.Data))

	return &pb.WorkerResponse{

			Success: true,
		},

		nil

}

func (w *WorkerNode) GetBlock(ctx context.Context, req *pb.GetBlockRequest) (*pb.GetBlockResponse, error) {

	blockID := req.BlockId

	log.Printf("Received GetBlock request for block ID: %s", blockID)

	w.lock.Lock()

	blockData, exists := w.StoredBlocks[blockID]

	w.lock.Unlock()

	if !exists {

		filePath := filepath.Join("/data", fmt.Sprintf("%s.bin", blockID))

		data, err := os.ReadFile(filePath)

		if err != nil {

			log.Printf("Block not found in memory or disk: %s", blockID)

			return &pb.GetBlockResponse{}, fmt.Errorf("block not found")

		}

		blockData = data

		w.lock.Lock()

		w.StoredBlocks[blockID] = blockData

		w.lock.Unlock()

	}

	return &pb.GetBlockResponse{

			Data: blockData,
		},

		nil

}
