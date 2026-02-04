package nodes

import (
	"bufio"
	"context"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/razvanmarinn/dfs/internal/metrics"
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
	hasher := crc32.NewIEEE()
	startTime := time.Now()

	defer func() {
		if file != nil {
			file.Close()
		}
	}()

	for {
		req, err := stream.Recv()
		if err == io.EOF {
			checksum := hasher.Sum32()
			checksumDuration := time.Since(startTime).Seconds()

			log.Printf("✅ Stored Block %s (%d bytes, checksum: %d)", blockID, totalBytes, checksum)

			checksumFilePath := filepath.Join(wn.StorageDir, fmt.Sprintf("%s.checksum", blockID))
			if err := os.WriteFile(checksumFilePath, []byte(fmt.Sprintf("%d", checksum)), 0644); err != nil {
				log.Printf("Warning: Failed to write checksum file for block %s: %v", blockID, err)
				metrics.BlockWritesTotal.WithLabelValues("failure").Inc()
				return err
			}

			metrics.BlockWritesTotal.WithLabelValues("success").Inc()
			metrics.ChecksumCalculationDuration.Observe(checksumDuration)
			metrics.BlockWriteSizeBytes.Observe(float64(totalBytes))

			return stream.SendAndClose(&datanodev1.PushBlockResponse{
				Success: true,
				Message: fmt.Sprintf("Stored %d bytes, checksum: %d", totalBytes, checksum),
			})
		}
		if err != nil {
			log.Printf("Stream error: %v", err)
			metrics.BlockWritesTotal.WithLabelValues("failure").Inc()
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
			hasher.Write(payload.Chunk)
			totalBytes += int64(n)
		}
	}
}

func (wn *WorkerNode) getStoredChecksum(blockID string) (uint32, error) {
	checksumFilePath := filepath.Join(wn.StorageDir, fmt.Sprintf("%s.checksum", blockID))
	data, err := os.ReadFile(checksumFilePath)
	if err != nil {
		return 0, err
	}

	var checksum uint32
	_, err = fmt.Sscanf(string(data), "%d", &checksum)
	if err != nil {
		return 0, fmt.Errorf("failed to parse checksum: %w", err)
	}

	return checksum, nil
}

func (wn *WorkerNode) verifyBlockIntegrity(blockID string) error {
	startTime := time.Now()
	filePath := filepath.Join(wn.StorageDir, fmt.Sprintf("%s.bin", blockID))

	file, err := os.Open(filePath)
	if err != nil {
		metrics.ChecksumVerificationsTotal.WithLabelValues("missing").Inc()
		return fmt.Errorf("failed to open block file: %w", err)
	}
	defer file.Close()

	hasher := crc32.NewIEEE()
	if _, err := io.Copy(hasher, file); err != nil {
		metrics.ChecksumVerificationsTotal.WithLabelValues("error").Inc()
		return fmt.Errorf("failed to calculate checksum: %w", err)
	}

	calculatedChecksum := hasher.Sum32()
	storedChecksum, err := wn.getStoredChecksum(blockID)
	if err != nil {
		metrics.ChecksumVerificationsTotal.WithLabelValues("missing").Inc()
		return fmt.Errorf("failed to retrieve stored checksum: %w", err)
	}

	verificationDuration := time.Since(startTime).Seconds()
	metrics.ChecksumVerificationDuration.Observe(verificationDuration)

	if calculatedChecksum != storedChecksum {
		metrics.ChecksumVerificationsTotal.WithLabelValues("corrupted").Inc()
		metrics.BlockCorruptionTotal.WithLabelValues(wn.ID).Inc()
		return fmt.Errorf("checksum mismatch: calculated=%d, stored=%d (CORRUPTION DETECTED)",
			calculatedChecksum, storedChecksum)
	}

	metrics.ChecksumVerificationsTotal.WithLabelValues("valid").Inc()
	log.Printf("✓ Block %s integrity verified (checksum: %d)", blockID, calculatedChecksum)
	return nil
}

func (wn *WorkerNode) FetchBlock(req *datanodev1.FetchBlockRequest, stream datanodev1.DataNodeService_FetchBlockServer) error {
	blockID := req.BlockId
	filePath := filepath.Join(wn.StorageDir, fmt.Sprintf("%s.bin", blockID))
	var totalBytes int64

	if err := wn.verifyBlockIntegrity(blockID); err != nil {
		log.Printf("⚠️ Block integrity check failed for %s: %v", blockID, err)
		metrics.BlockReadsTotal.WithLabelValues("failure").Inc()
		return fmt.Errorf("block integrity check failed: %w", err)
	}

	file, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			log.Printf("Block not found: %s", blockID)
			metrics.BlockReadsTotal.WithLabelValues("failure").Inc()
			return fmt.Errorf("block not found")
		}
		metrics.BlockReadsTotal.WithLabelValues("failure").Inc()
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
			metrics.BlockReadsTotal.WithLabelValues("failure").Inc()
			return err
		}

		totalBytes += int64(n)

		if err := stream.Send(&datanodev1.FetchBlockResponse{
			Chunk: buffer[:n],
		}); err != nil {
			metrics.BlockReadsTotal.WithLabelValues("failure").Inc()
			return err
		}
	}

	metrics.BlockReadsTotal.WithLabelValues("success").Inc()
	metrics.BlockReadSizeBytes.Observe(float64(totalBytes))

	return nil
}

func (wn *WorkerNode) GetBlockChecksum(ctx context.Context, req *datanodev1.GetBlockChecksumRequest) (*datanodev1.GetBlockChecksumResponse, error) {
	blockID := req.BlockId

	checksum, err := wn.getStoredChecksum(blockID)
	if err != nil {
		if os.IsNotExist(err) {
			return &datanodev1.GetBlockChecksumResponse{
				Checksum: 0,
				Exists:   false,
			}, nil
		}
		return nil, fmt.Errorf("failed to retrieve checksum: %w", err)
	}

	return &datanodev1.GetBlockChecksumResponse{
		Checksum: checksum,
		Exists:   true,
	}, nil
}

func (wn *WorkerNode) DeleteBlock(ctx context.Context, req *datanodev1.DeleteBlockRequest) (*datanodev1.DeleteBlockResponse, error) {
	blockID := req.BlockId
	filePath := filepath.Join(wn.StorageDir, fmt.Sprintf("%s.bin", blockID))
	checksumFilePath := filepath.Join(wn.StorageDir, fmt.Sprintf("%s.checksum", blockID))

	log.Printf("🗑️ Deleting Block %s", blockID)

	if err := os.Remove(filePath); err != nil {
		if os.IsNotExist(err) {
			log.Printf("Block %s not found during deletion (already deleted?)", blockID)
			os.Remove(checksumFilePath)
			return &datanodev1.DeleteBlockResponse{Success: true, Message: "Block not found, assumed deleted"}, nil
		}
		log.Printf("Failed to delete block %s: %v", blockID, err)
		return &datanodev1.DeleteBlockResponse{Success: false, Message: err.Error()}, err
	}

	if err := os.Remove(checksumFilePath); err != nil && !os.IsNotExist(err) {
		log.Printf("Warning: Failed to delete checksum file for block %s: %v", blockID, err)
	}

	return &datanodev1.DeleteBlockResponse{Success: true, Message: "Block deleted successfully"}, nil
}
