package nodes

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"

	"github.com/google/uuid"
	datanodev1 "github.com/razvanmarinn/datalake/protobuf/gen/go/datanode/v1"
)

type WorkerNode struct {
	ID         string
	StorageDir string
	Port       int
	Address    string
	lock       sync.Mutex

	datanodev1.UnimplementedDataNodeServiceServer
}

func NewWorkerNode(storageDir string, port int) *WorkerNode {
	if err := os.MkdirAll(storageDir, 0755); err != nil {
		log.Fatalf("Failed to create storage dir: %v", err)
	}

	hostname, err := os.Hostname()
	if err != nil {
		hostname = "localhost"
	}

	dnsAddress := fmt.Sprintf("%s.worker-headless:%d", hostname, port)
	if hostname == "localhost" {
		dnsAddress = fmt.Sprintf("localhost:%d", port)
	}

	return &WorkerNode{
		ID:         uuid.New().String(),
		StorageDir: storageDir,
		Port:       port,
		Address:    dnsAddress,
	}
}

func (wn *WorkerNode) Start() {
	fmt.Printf("WorkerNode started at %s (Port: %d)\n", wn.StorageDir, wn.Port)
}

func (wn *WorkerNode) Stop() {
}

func (wn *WorkerNode) HealthCheck() bool {
	testFile := filepath.Join(wn.StorageDir, ".health")
	err := os.WriteFile(testFile, []byte("ok"), 0644)
	return err == nil
}

func (wn *WorkerNode) GetWorkerInfo(ctx context.Context, req *datanodev1.GetWorkerInfoRequest) (*datanodev1.GetWorkerInfoResponse, error) {
	return &datanodev1.GetWorkerInfoResponse{
		WorkerId: wn.ID,
		Address:  wn.Address,
	}, nil
}

func (wn *WorkerNode) PushBlock(stream datanodev1.DataNodeService_PushBlockServer) error {
	var file *os.File
	var blockID string
	var totalBytes int64

	defer func() {
		if file != nil {
			file.Close()
		}
	}()

	for {
		req, err := stream.Recv()
		if err == io.EOF {
			log.Printf("✅ Stored Block %s (%d bytes)", blockID, totalBytes)
			return stream.SendAndClose(&datanodev1.PushBlockResponse{
				Success: true,
				Message: fmt.Sprintf("Stored %d bytes", totalBytes),
			})
		}
		if err != nil {
			log.Printf("Stream error: %v", err)
			return err
		}

		switch payload := req.Data.(type) {

		case *datanodev1.PushBlockRequest_Metadata:
			blockID = payload.Metadata.BlockId
			filePath := filepath.Join(wn.StorageDir, fmt.Sprintf("%s.bin", blockID))

			log.Printf("📥 Starting upload for Block %s", blockID)

			f, err := os.Create(filePath)
			if err != nil {
				return fmt.Errorf("failed to create file: %w", err)
			}
			file = f

		case *datanodev1.PushBlockRequest_Chunk:
			if file == nil {
				return fmt.Errorf("received chunk before metadata")
			}

			n, err := file.Write(payload.Chunk)
			if err != nil {
				return fmt.Errorf("write failure: %w", err)
			}
			totalBytes += int64(n)
		}
	}
}

// --- UPDATED RPC: FetchBlock ---
// Note: Type changed from DfsService_... to DataNodeService_...
func (wn *WorkerNode) FetchBlock(req *datanodev1.FetchBlockRequest, stream datanodev1.DataNodeService_FetchBlockServer) error {
	blockID := req.BlockId
	filePath := filepath.Join(wn.StorageDir, fmt.Sprintf("%s.bin", blockID))

	file, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			log.Printf("Block not found: %s", blockID)
			return fmt.Errorf("block not found")
		}
		return err
	}
	defer file.Close()

	log.Printf("📤 Streaming Block %s to client...", blockID)

	buffer := make([]byte, 64*1024)
	reader := bufio.NewReader(file)

	for {
		n, err := reader.Read(buffer)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		if err := stream.Send(&datanodev1.FetchBlockResponse{
			Chunk: buffer[:n],
		}); err != nil {
			return err
		}
	}

	return nil
}
