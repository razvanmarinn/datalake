package nodes

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	commonv1 "github.com/razvanmarinn/datalake/protobuf/gen/go/common/v1"
	coordinatorv1 "github.com/razvanmarinn/datalake/protobuf/gen/go/coordinator/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupTestMaster(t *testing.T) *MasterNode {
	tmpDir := t.TempDir()

	// Create master with temporary directory
	logPath := filepath.Join(tmpDir, "master_op.log")
	f, err := os.OpenFile(logPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	require.NoError(t, err)

	master := &MasterNode{
		ID:        "test-master",
		Namespace: make(map[string]*Inode),
		BlockMap:  make(map[uuid.UUID]*BlockMetadata),
		opLogFile: f,
	}

	t.Cleanup(func() {
		f.Close()
	})

	return master
}

func TestMasterNode_AllocateBlock(t *testing.T) {
	master := setupTestMaster(t)

	t.Run("master has block map", func(t *testing.T) {
		assert.NotNil(t, master.BlockMap)
		assert.NotNil(t, master.Namespace)
		assert.Equal(t, 0, len(master.BlockMap))
	})

	t.Run("can add blocks to block map", func(t *testing.T) {
		blockID := uuid.New()
		master.BlockMap[blockID] = &BlockMetadata{
			BlockID:  blockID,
			Size:     1024 * 1024,
			Checksum: 12345,
			Replicas: []uuid.UUID{uuid.New()},
		}

		assert.Equal(t, 1, len(master.BlockMap))
		assert.Contains(t, master.BlockMap, blockID)
	})
}

func TestMasterNode_GetFileBatches(t *testing.T) {
	master := setupTestMaster(t)

	// Setup test data
	testPath := "/test/file.txt"
	blockIDs := []uuid.UUID{
		uuid.New(),
		uuid.New(),
		uuid.New(),
	}

	master.Namespace[testPath] = &Inode{
		ID:     "test-inode",
		Name:   "file.txt",
		Path:   testPath,
		Type:   FileType,
		Blocks: blockIDs,
	}

	t.Run("get existing file batches", func(t *testing.T) {
		batches := master.GetFileBatches(testPath)
		assert.NotNil(t, batches)
		assert.Equal(t, 3, len(batches))
		assert.Equal(t, blockIDs, batches)
	})

	t.Run("get non-existent file batches", func(t *testing.T) {
		batches := master.GetFileBatches("/non/existent/file")
		assert.Nil(t, batches)
	})
}

func TestMasterNode_GetBatchLocations(t *testing.T) {
	master := setupTestMaster(t)

	blockID := uuid.New()
	workerIDs := []uuid.UUID{
		uuid.New(),
		uuid.New(),
	}

	master.BlockMap[blockID] = &BlockMetadata{
		BlockID:  blockID,
		Size:     1024,
		Checksum: 12345,
		Replicas: workerIDs,
	}

	t.Run("get existing block locations", func(t *testing.T) {
		locations := master.GetBatchLocations(blockID)
		assert.NotNil(t, locations)
		assert.Equal(t, 2, len(locations))
		assert.Equal(t, workerIDs, locations)
	})

	t.Run("get non-existent block locations", func(t *testing.T) {
		locations := master.GetBatchLocations(uuid.New())
		assert.Nil(t, locations)
	})
}

func TestMasterNode_UpdateBlockLocation(t *testing.T) {
	master := setupTestMaster(t)

	blockID := uuid.New()
	workerID1 := uuid.New()
	workerID2 := uuid.New()

	master.BlockMap[blockID] = &BlockMetadata{
		BlockID:  blockID,
		Size:     1024,
		Replicas: []uuid.UUID{workerID1},
	}

	t.Run("add new worker location", func(t *testing.T) {
		master.UpdateBlockLocation(blockID, workerID2.String())

		blockMeta := master.BlockMap[blockID]
		assert.Equal(t, 2, len(blockMeta.Replicas))
		assert.Contains(t, blockMeta.Replicas, workerID1)
		assert.Contains(t, blockMeta.Replicas, workerID2)
	})

	t.Run("add duplicate worker location", func(t *testing.T) {
		initialLen := len(master.BlockMap[blockID].Replicas)
		master.UpdateBlockLocation(blockID, workerID1.String())

		// Should not add duplicate
		assert.Equal(t, initialLen, len(master.BlockMap[blockID].Replicas))
	})

	t.Run("update non-existent block", func(t *testing.T) {
		// Should not panic
		master.UpdateBlockLocation(uuid.New(), uuid.New().String())
	})
}

func TestMasterNode_GetWorkerAllocatedForBatch(t *testing.T) {
	master := setupTestMaster(t)

	blockID := uuid.New()
	workerID := uuid.New()

	master.BlockMap[blockID] = &BlockMetadata{
		BlockID:  blockID,
		Size:     1024,
		Replicas: []uuid.UUID{workerID},
	}

	t.Run("get worker for existing block", func(t *testing.T) {
		worker, err := master.GetWorkerAllocatedForBatch(blockID)
		assert.NoError(t, err)
		assert.Equal(t, workerID.String(), worker)
	})

	t.Run("get worker for non-existent block", func(t *testing.T) {
		_, err := master.GetWorkerAllocatedForBatch(uuid.New())
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not found")
	})

	t.Run("get worker for block with no replicas", func(t *testing.T) {
		emptyBlockID := uuid.New()
		master.BlockMap[emptyBlockID] = &BlockMetadata{
			BlockID:  emptyBlockID,
			Size:     1024,
			Replicas: []uuid.UUID{},
		}

		_, err := master.GetWorkerAllocatedForBatch(emptyBlockID)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "no workers allocated")
	})
}

func TestMasterNode_CommitFileInternal(t *testing.T) {
	master := setupTestMaster(t)

	block1 := uuid.New()
	block2 := uuid.New()

	// Add blocks to BlockMap
	master.BlockMap[block1] = &BlockMetadata{
		BlockID:  block1,
		Size:     1024,
		Checksum: 11111,
		Replicas: []uuid.UUID{uuid.New()},
	}
	master.BlockMap[block2] = &BlockMetadata{
		BlockID:  block2,
		Size:     2048,
		Checksum: 22222,
		Replicas: []uuid.UUID{uuid.New()},
	}

	req := &coordinatorv1.CommitFileRequest{
		ProjectId: "test-project",
		FilePath:  "data/test-file.txt",
		Blocks: []*commonv1.BlockInfo{
			{BlockId: block1.String(), Size: 1024, Checksum: 11111},
			{BlockId: block2.String(), Size: 2048, Checksum: 22222},
		},
		FileFormat: "txt",
		OwnerId:    "user-123",
	}

	t.Run("commit file successfully", func(t *testing.T) {
		inode, err := master.commitFileInternal(req)
		assert.NoError(t, err)
		assert.NotNil(t, inode)
		assert.Equal(t, "test-file.txt", inode.Name)
		assert.Equal(t, "data/test-file.txt", inode.Path)
		assert.Equal(t, FileType, inode.Type)
		assert.Equal(t, "test-project", inode.ProjectID)
		assert.Equal(t, "user-123", inode.OwnerID)
		assert.Equal(t, int64(3072), inode.Size) // 1024 + 2048
		assert.Equal(t, 2, len(inode.Blocks))
	})

	t.Run("commit file creates directory", func(t *testing.T) {
		// Check that directory was created
		_, exists := master.Namespace["data"]
		assert.True(t, exists)
	})

	t.Run("commit file with invalid project", func(t *testing.T) {
		invalidReq := &coordinatorv1.CommitFileRequest{
			ProjectId: "",
			FilePath:  "test.txt",
		}

		_, err := master.commitFileInternal(invalidReq)
		assert.Error(t, err)
	})

	t.Run("commit file with invalid block UUID", func(t *testing.T) {
		invalidReq := &coordinatorv1.CommitFileRequest{
			ProjectId: "test-project",
			FilePath:  "test.txt",
			Blocks: []*commonv1.BlockInfo{
				{BlockId: "invalid-uuid", Size: 1024},
			},
		}

		_, err := master.commitFileInternal(invalidReq)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid block uuid")
	})
}

func TestMasterNode_ListFiles(t *testing.T) {
	master := setupTestMaster(t)

	// Add test files
	master.Namespace["project1/file1.txt"] = &Inode{
		ID:        "inode1",
		Name:      "file1.txt",
		Path:      "project1/file1.txt",
		Type:      FileType,
		ProjectID: "project1",
	}
	master.Namespace["project1/file2.txt"] = &Inode{
		ID:        "inode2",
		Name:      "file2.txt",
		Path:      "project1/file2.txt",
		Type:      FileType,
		ProjectID: "project1",
	}
	master.Namespace["project1/data/file3.txt"] = &Inode{
		ID:        "inode3",
		Name:      "file3.txt",
		Path:      "project1/data/file3.txt",
		Type:      FileType,
		ProjectID: "project1",
	}
	master.Namespace["project2/file4.txt"] = &Inode{
		ID:        "inode4",
		Name:      "file4.txt",
		Path:      "project2/file4.txt",
		Type:      FileType,
		ProjectID: "project2",
	}

	t.Run("list all files in project", func(t *testing.T) {
		files, err := master.ListFiles("project1", "")
		assert.NoError(t, err)
		assert.Equal(t, 3, len(files))
	})

	t.Run("list files with prefix", func(t *testing.T) {
		files, err := master.ListFiles("project1", "data")
		assert.NoError(t, err)
		assert.Equal(t, 1, len(files))
		assert.Contains(t, files[0], "file3.txt")
	})

	t.Run("list files in empty project", func(t *testing.T) {
		files, err := master.ListFiles("empty-project", "")
		assert.NoError(t, err)
		assert.Equal(t, 0, len(files))
	})
}

func TestMasterNode_ConcurrentReads(t *testing.T) {
	master := setupTestMaster(t)

	// Setup test data
	for i := 0; i < 100; i++ {
		path := filepath.Join("test", "file"+string(rune(i))+".txt")
		master.Namespace[path] = &Inode{
			ID:     uuid.New().String(),
			Name:   "file.txt",
			Path:   path,
			Type:   FileType,
			Blocks: []uuid.UUID{uuid.New()},
		}
	}

	t.Run("concurrent GetFileBatches", func(t *testing.T) {
		done := make(chan bool, 10)

		for i := 0; i < 10; i++ {
			go func(idx int) {
				path := filepath.Join("test", "file"+string(rune(idx))+".txt")
				batches := master.GetFileBatches(path)
				assert.NotNil(t, batches)
				done <- true
			}(i)
		}

		for i := 0; i < 10; i++ {
			<-done
		}
	})

	t.Run("concurrent ListFiles", func(t *testing.T) {
		done := make(chan bool, 10)

		for i := 0; i < 10; i++ {
			go func() {
				files, err := master.ListFiles("test", "")
				assert.NoError(t, err)
				assert.NotNil(t, files)
				done <- true
			}()
		}

		for i := 0; i < 10; i++ {
			<-done
		}
	})
}

func TestMasterNode_HealthCheck(t *testing.T) {
	master := setupTestMaster(t)

	t.Run("health check returns true", func(t *testing.T) {
		healthy := master.HealthCheck()
		assert.True(t, healthy)
	})
}
