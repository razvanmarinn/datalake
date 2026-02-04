package nodes

import (
	"context"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/google/uuid"
	datanodev1 "github.com/razvanmarinn/datalake/protobuf/gen/go/datanode/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInode_Structure(t *testing.T) {
	t.Run("file inode", func(t *testing.T) {
		blockID := uuid.New()
		inode := &Inode{
			ID:        "inode-1",
			Name:      "data.parquet",
			Path:      "/project/schema/data.parquet",
			OwnerID:   "owner-123",
			Type:      FileType,
			ProjectID: "project-1",
			Size:      1024,
			Blocks:    []uuid.UUID{blockID},
			Children:  []string{},
		}
		assert.Equal(t, FileType, inode.Type)
		assert.Len(t, inode.Blocks, 1)
		assert.Equal(t, blockID, inode.Blocks[0])
	})

	t.Run("directory inode", func(t *testing.T) {
		inode := &Inode{
			ID:       "inode-2",
			Name:     "schema",
			Path:     "/project/schema",
			Type:     DirType,
			Children: []string{"file1.parquet", "file2.parquet"},
		}
		assert.Equal(t, DirType, inode.Type)
		assert.Len(t, inode.Children, 2)
	})
}

func TestBlockMetadata_Structure(t *testing.T) {
	t.Run("create with all fields", func(t *testing.T) {
		blockID := uuid.New()
		worker1 := uuid.New()
		worker2 := uuid.New()

		bm := &BlockMetadata{
			BlockID:  blockID,
			Size:     64 * 1024 * 1024,
			Checksum: 0xDEADBEEF,
			Version:  1,
			Replicas: []uuid.UUID{worker1, worker2},
		}

		assert.Equal(t, blockID, bm.BlockID)
		assert.Equal(t, int64(64*1024*1024), bm.Size)
		assert.Equal(t, uint32(0xDEADBEEF), bm.Checksum)
		assert.Len(t, bm.Replicas, 2)
	})

	t.Run("add replicas", func(t *testing.T) {
		bm := &BlockMetadata{
			BlockID:  uuid.New(),
			Replicas: []uuid.UUID{},
		}
		for i := 0; i < 3; i++ {
			bm.Replicas = append(bm.Replicas, uuid.New())
		}
		assert.Len(t, bm.Replicas, 3)
	})
}

func TestWorkerNode_BlockStorage(t *testing.T) {
	tmpDir := t.TempDir()
	wn := NewWorkerNode(tmpDir, 50051)

	t.Run("store and verify block", func(t *testing.T) {
		blockID := "storage-test-block"
		data := []byte("test block data content for storage test")
		checksum := crc32.ChecksumIEEE(data)

		blockPath := filepath.Join(tmpDir, fmt.Sprintf("%s.bin", blockID))
		err := os.WriteFile(blockPath, data, 0644)
		require.NoError(t, err)

		checksumPath := filepath.Join(tmpDir, fmt.Sprintf("%s.checksum", blockID))
		err = os.WriteFile(checksumPath, []byte(fmt.Sprintf("%d", checksum)), 0644)
		require.NoError(t, err)

		err = wn.verifyBlockIntegrity(blockID)
		assert.NoError(t, err)
	})

	t.Run("detect corruption", func(t *testing.T) {
		blockID := "corrupted-block-test"
		data := []byte("original data")
		wrongChecksum := uint32(99999)

		blockPath := filepath.Join(tmpDir, fmt.Sprintf("%s.bin", blockID))
		os.WriteFile(blockPath, data, 0644)

		checksumPath := filepath.Join(tmpDir, fmt.Sprintf("%s.checksum", blockID))
		os.WriteFile(checksumPath, []byte(fmt.Sprintf("%d", wrongChecksum)), 0644)

		err := wn.verifyBlockIntegrity(blockID)
		assert.Error(t, err)
	})
}

func TestWorkerNode_GetBlockChecksum(t *testing.T) {
	tmpDir := t.TempDir()
	wn := NewWorkerNode(tmpDir, 50051)

	t.Run("existing block", func(t *testing.T) {
		blockID := "checksum-test-block"
		data := []byte("checksum test data")
		checksum := crc32.ChecksumIEEE(data)

		blockPath := filepath.Join(tmpDir, fmt.Sprintf("%s.bin", blockID))
		os.WriteFile(blockPath, data, 0644)

		checksumPath := filepath.Join(tmpDir, fmt.Sprintf("%s.checksum", blockID))
		os.WriteFile(checksumPath, []byte(fmt.Sprintf("%d", checksum)), 0644)

		resp, err := wn.GetBlockChecksum(context.Background(), &datanodev1.GetBlockChecksumRequest{
			BlockId: blockID,
		})
		require.NoError(t, err)
		assert.True(t, resp.Exists)
		assert.Equal(t, checksum, resp.Checksum)
	})

	t.Run("non-existent block", func(t *testing.T) {
		resp, err := wn.GetBlockChecksum(context.Background(), &datanodev1.GetBlockChecksumRequest{
			BlockId: "nonexistent-block-xyz",
		})
		require.NoError(t, err)
		assert.False(t, resp.Exists)
	})
}

func TestWorkerNode_DeleteBlock(t *testing.T) {
	tmpDir := t.TempDir()
	wn := NewWorkerNode(tmpDir, 50051)

	t.Run("delete existing block", func(t *testing.T) {
		blockID := "delete-test-block-2"
		data := []byte("data to delete")
		checksum := crc32.ChecksumIEEE(data)

		blockPath := filepath.Join(tmpDir, fmt.Sprintf("%s.bin", blockID))
		os.WriteFile(blockPath, data, 0644)

		checksumPath := filepath.Join(tmpDir, fmt.Sprintf("%s.checksum", blockID))
		os.WriteFile(checksumPath, []byte(fmt.Sprintf("%d", checksum)), 0644)

		resp, err := wn.DeleteBlock(context.Background(), &datanodev1.DeleteBlockRequest{
			BlockId: blockID,
		})
		require.NoError(t, err)
		assert.True(t, resp.Success)

		_, err = os.Stat(blockPath)
		assert.True(t, os.IsNotExist(err))
	})
}

func TestWorkerNode_GetWorkerInfo(t *testing.T) {
	tmpDir := t.TempDir()
	wn := NewWorkerNode(tmpDir, 50051)

	resp, err := wn.GetWorkerInfo(context.Background(), &datanodev1.GetWorkerInfoRequest{})
	require.NoError(t, err)
	assert.NotEmpty(t, resp.WorkerId)
	assert.Contains(t, resp.Address, "50051")
}

func TestWorkerNode_HealthCheck(t *testing.T) {
	tmpDir := t.TempDir()
	wn := NewWorkerNode(tmpDir, 50051)

	t.Run("healthy storage", func(t *testing.T) {
		healthy := wn.HealthCheck()
		assert.True(t, healthy)
	})

	t.Run("creates health file", func(t *testing.T) {
		wn.HealthCheck()
		healthFile := filepath.Join(tmpDir, ".health")
		_, err := os.Stat(healthFile)
		assert.NoError(t, err)
	})
}

func TestWorkerNode_ConcurrentOperations(t *testing.T) {
	tmpDir := t.TempDir()
	wn := NewWorkerNode(tmpDir, 50051)

	t.Run("concurrent health checks", func(t *testing.T) {
		var wg sync.WaitGroup
		for i := 0; i < 20; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				healthy := wn.HealthCheck()
				assert.True(t, healthy)
			}()
		}
		wg.Wait()
	})

	t.Run("concurrent block verifications", func(t *testing.T) {
		for i := 0; i < 3; i++ {
			blockID := fmt.Sprintf("concurrent-verify-block-%d", i)
			data := []byte(fmt.Sprintf("data for block %d", i))
			checksum := crc32.ChecksumIEEE(data)

			blockPath := filepath.Join(tmpDir, fmt.Sprintf("%s.bin", blockID))
			os.WriteFile(blockPath, data, 0644)

			checksumPath := filepath.Join(tmpDir, fmt.Sprintf("%s.checksum", blockID))
			os.WriteFile(checksumPath, []byte(fmt.Sprintf("%d", checksum)), 0644)
		}

		var wg sync.WaitGroup
		for i := 0; i < 3; i++ {
			for j := 0; j < 3; j++ {
				wg.Add(1)
				go func(blockNum int) {
					defer wg.Done()
					blockID := fmt.Sprintf("concurrent-verify-block-%d", blockNum)
					wn.verifyBlockIntegrity(blockID)
				}(i)
			}
		}
		wg.Wait()
	})
}

func TestChecksumCalculation(t *testing.T) {
	t.Run("consistent checksum", func(t *testing.T) {
		data := []byte("consistent checksum test data")
		checksum1 := crc32.ChecksumIEEE(data)
		checksum2 := crc32.ChecksumIEEE(data)
		assert.Equal(t, checksum1, checksum2)
	})

	t.Run("different data different checksum", func(t *testing.T) {
		data1 := []byte("data one")
		data2 := []byte("data two")
		checksum1 := crc32.ChecksumIEEE(data1)
		checksum2 := crc32.ChecksumIEEE(data2)
		assert.NotEqual(t, checksum1, checksum2)
	})

	t.Run("large data checksum", func(t *testing.T) {
		largeData := make([]byte, 5*1024*1024) // 5MB
		for i := range largeData {
			largeData[i] = byte(i % 256)
		}
		checksum := crc32.ChecksumIEEE(largeData)
		assert.NotZero(t, checksum)
	})
}

func TestInodeType_Constants(t *testing.T) {
	t.Run("file type", func(t *testing.T) {
		assert.Equal(t, InodeType(0), FileType)
	})

	t.Run("dir type", func(t *testing.T) {
		assert.Equal(t, InodeType(1), DirType)
	})
}
