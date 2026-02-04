package nodes

import (
	"context"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sync"
	"testing"

	datanodev1 "github.com/razvanmarinn/datalake/protobuf/gen/go/datanode/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWorkerNode_ConcurrentWrites(t *testing.T) {
	tmpDir := t.TempDir()

	t.Run("concurrent block writes", func(t *testing.T) {
		var wg sync.WaitGroup
		numWrites := 10

		for i := 0; i < numWrites; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()

				blockID := fmt.Sprintf("concurrent-block-%d", idx)
				data := []byte(fmt.Sprintf("Test data for block %d", idx))
				checksum := crc32.ChecksumIEEE(data)

				// Write block file
				blockPath := filepath.Join(tmpDir, fmt.Sprintf("%s.bin", blockID))
				err := os.WriteFile(blockPath, data, 0644)
				require.NoError(t, err)

				// Write checksum file
				checksumPath := filepath.Join(tmpDir, fmt.Sprintf("%s.checksum", blockID))
				err = os.WriteFile(checksumPath, []byte(fmt.Sprintf("%d", checksum)), 0644)
				require.NoError(t, err)
			}(i)
		}

		wg.Wait()

		// Verify all blocks were written
		files, err := os.ReadDir(tmpDir)
		require.NoError(t, err)

		blockCount := 0
		for _, file := range files {
			if filepath.Ext(file.Name()) == ".bin" {
				blockCount++
			}
		}
		assert.Equal(t, numWrites, blockCount)
	})
}

func TestWorkerNode_ConcurrentReads(t *testing.T) {
	tmpDir := t.TempDir()
	worker := NewWorkerNode(tmpDir, 50051)

	// Setup test blocks
	numBlocks := 5
	for i := 0; i < numBlocks; i++ {
		blockID := fmt.Sprintf("read-block-%d", i)
		data := []byte(fmt.Sprintf("Test data %d", i))
		checksum := crc32.ChecksumIEEE(data)

		blockPath := filepath.Join(tmpDir, fmt.Sprintf("%s.bin", blockID))
		os.WriteFile(blockPath, data, 0644)

		checksumPath := filepath.Join(tmpDir, fmt.Sprintf("%s.checksum", blockID))
		os.WriteFile(checksumPath, []byte(fmt.Sprintf("%d", checksum)), 0644)
	}

	t.Run("concurrent integrity checks", func(t *testing.T) {
		var wg sync.WaitGroup
		errors := make(chan error, numBlocks*3)

		for i := 0; i < numBlocks; i++ {
			for j := 0; j < 3; j++ {
				wg.Add(1)
				go func(idx int) {
					defer wg.Done()
					blockID := fmt.Sprintf("read-block-%d", idx)
					err := worker.verifyBlockIntegrity(blockID)
					if err != nil {
						errors <- err
					}
				}(i)
			}
		}

		wg.Wait()
		close(errors)

		// Check no errors occurred
		for err := range errors {
			t.Errorf("Integrity check failed: %v", err)
		}
	})
}

func TestWorkerNode_ErrorHandling(t *testing.T) {
	tmpDir := t.TempDir()
	worker := NewWorkerNode(tmpDir, 50051)

	t.Run("verify missing block file", func(t *testing.T) {
		err := worker.verifyBlockIntegrity("non-existent-block")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to open block file")
	})

	t.Run("verify missing checksum file", func(t *testing.T) {
		blockID := "block-no-checksum"
		blockPath := filepath.Join(tmpDir, fmt.Sprintf("%s.bin", blockID))
		os.WriteFile(blockPath, []byte("test data"), 0644)

		err := worker.verifyBlockIntegrity(blockID)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to retrieve stored checksum")
	})

	t.Run("verify corrupted checksum file", func(t *testing.T) {
		blockID := "block-bad-checksum"
		blockPath := filepath.Join(tmpDir, fmt.Sprintf("%s.bin", blockID))
		os.WriteFile(blockPath, []byte("test data"), 0644)

		checksumPath := filepath.Join(tmpDir, fmt.Sprintf("%s.checksum", blockID))
		os.WriteFile(checksumPath, []byte("not-a-number"), 0644)

		err := worker.verifyBlockIntegrity(blockID)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to retrieve stored checksum")
	})

	t.Run("get checksum for non-existent block", func(t *testing.T) {
		resp, err := worker.GetBlockChecksum(context.Background(), &datanodev1.GetBlockChecksumRequest{
			BlockId: "non-existent",
		})
		require.NoError(t, err)
		assert.False(t, resp.Exists)
		assert.Equal(t, uint32(0), resp.Checksum)
	})
}


func TestWorkerNode_DeleteBlockCleanup(t *testing.T) {
	tmpDir := t.TempDir()
	worker := NewWorkerNode(tmpDir, 50051)

	blockID := "block-to-delete"
	data := []byte("test data")
	checksum := crc32.ChecksumIEEE(data)

	blockPath := filepath.Join(tmpDir, fmt.Sprintf("%s.bin", blockID))
	os.WriteFile(blockPath, data, 0644)

	checksumPath := filepath.Join(tmpDir, fmt.Sprintf("%s.checksum", blockID))
	os.WriteFile(checksumPath, []byte(fmt.Sprintf("%d", checksum)), 0644)

	t.Run("delete removes both files", func(t *testing.T) {
		resp, err := worker.DeleteBlock(context.Background(), &datanodev1.DeleteBlockRequest{
			BlockId: blockID,
		})
		require.NoError(t, err)
		assert.True(t, resp.Success)

		// Verify both files are gone
		_, err = os.Stat(blockPath)
		assert.True(t, os.IsNotExist(err))

		_, err = os.Stat(checksumPath)
		assert.True(t, os.IsNotExist(err))
	})

	t.Run("delete non-existent block succeeds", func(t *testing.T) {
		resp, err := worker.DeleteBlock(context.Background(), &datanodev1.DeleteBlockRequest{
			BlockId: "non-existent",
		})
		require.NoError(t, err)
		assert.True(t, resp.Success)
	})
}

func TestWorkerNode_StorageDirectory(t *testing.T) {
	tmpDir := t.TempDir()
	worker := NewWorkerNode(tmpDir, 50051)

	t.Run("storage directory exists", func(t *testing.T) {
		info, err := os.Stat(worker.StorageDir)
		require.NoError(t, err)
		assert.True(t, info.IsDir())
	})

	t.Run("storage directory is writable", func(t *testing.T) {
		testFile := filepath.Join(worker.StorageDir, "test-write.tmp")
		err := os.WriteFile(testFile, []byte("test"), 0644)
		assert.NoError(t, err)
		os.Remove(testFile)
	})
}

func TestWorkerNode_ChecksumConsistency(t *testing.T) {
	tmpDir := t.TempDir()
	worker := NewWorkerNode(tmpDir, 50051)

	testData := []byte("Consistency test data")
	expectedChecksum := crc32.ChecksumIEEE(testData)

	t.Run("checksum calculation is consistent", func(t *testing.T) {
		// Calculate checksum multiple times
		for i := 0; i < 10; i++ {
			checksum := crc32.ChecksumIEEE(testData)
			assert.Equal(t, expectedChecksum, checksum, "Checksum should be consistent")
		}
	})

	t.Run("stored and calculated checksums match", func(t *testing.T) {
		blockID := "consistency-block"

		blockPath := filepath.Join(tmpDir, fmt.Sprintf("%s.bin", blockID))
		os.WriteFile(blockPath, testData, 0644)

		checksumPath := filepath.Join(tmpDir, fmt.Sprintf("%s.checksum", blockID))
		os.WriteFile(checksumPath, []byte(fmt.Sprintf("%d", expectedChecksum)), 0644)

		err := worker.verifyBlockIntegrity(blockID)
		assert.NoError(t, err)
	})
}

func TestWorkerNode_LargeBlockHandling(t *testing.T) {
	tmpDir := t.TempDir()
	worker := NewWorkerNode(tmpDir, 50051)

	t.Run("handle large block checksum", func(t *testing.T) {
		// Create a 10MB block
		largeData := make([]byte, 10*1024*1024)
		for i := range largeData {
			largeData[i] = byte(i % 256)
		}

		blockID := "large-block"
		checksum := crc32.ChecksumIEEE(largeData)

		blockPath := filepath.Join(tmpDir, fmt.Sprintf("%s.bin", blockID))
		err := os.WriteFile(blockPath, largeData, 0644)
		require.NoError(t, err)

		checksumPath := filepath.Join(tmpDir, fmt.Sprintf("%s.checksum", blockID))
		err = os.WriteFile(checksumPath, []byte(fmt.Sprintf("%d", checksum)), 0644)
		require.NoError(t, err)

		// Verify integrity
		err = worker.verifyBlockIntegrity(blockID)
		assert.NoError(t, err)
	})
}
