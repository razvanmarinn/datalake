package nodes

import (
	"context"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"testing"

	datanodev1 "github.com/razvanmarinn/datalake/protobuf/gen/go/datanode/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockPushBlockServer struct {
	datanodev1.DataNodeService_PushBlockServer
	requests []*datanodev1.PushBlockRequest
	response *datanodev1.PushBlockResponse
}

func (m *mockPushBlockServer) Recv() (*datanodev1.PushBlockRequest, error) {
	if len(m.requests) == 0 {
		return nil, io.EOF
	}
	req := m.requests[0]
	m.requests = m.requests[1:]
	return req, nil
}

func (m *mockPushBlockServer) SendAndClose(resp *datanodev1.PushBlockResponse) error {
	m.response = resp
	return nil
}

func TestChecksumCalculationOnWrite(t *testing.T) {
	tmpDir := t.TempDir()
	worker := NewWorkerNode(tmpDir, 50051)

	testData := []byte("Hello, this is test data for checksum verification!")
	expectedChecksum := crc32.ChecksumIEEE(testData)
	blockID := "test-block-123"

	mockServer := &mockPushBlockServer{
		requests: []*datanodev1.PushBlockRequest{
			{
				Data: &datanodev1.PushBlockRequest_Metadata{
					Metadata: &datanodev1.BlockMetadata{
						BlockId:   blockID,
						TotalSize: int64(len(testData)),
					},
				},
			},
			{
				Data: &datanodev1.PushBlockRequest_Chunk{
					Chunk: testData,
				},
			},
		},
	}

	err := worker.PushBlock(mockServer)
	require.NoError(t, err)
	require.NotNil(t, mockServer.response)
	assert.True(t, mockServer.response.Success)

	storedChecksum, err := worker.getStoredChecksum(blockID)
	require.NoError(t, err)
	assert.Equal(t, expectedChecksum, storedChecksum, "Stored checksum should match calculated checksum")

	blockFilePath := filepath.Join(tmpDir, fmt.Sprintf("%s.bin", blockID))
	assert.FileExists(t, blockFilePath)

	checksumFilePath := filepath.Join(tmpDir, fmt.Sprintf("%s.checksum", blockID))
	assert.FileExists(t, checksumFilePath)
}

func TestChecksumVerificationOnRead(t *testing.T) {
	tmpDir := t.TempDir()
	worker := NewWorkerNode(tmpDir, 50051)

	testData := []byte("Test data for read verification")
	blockID := "test-block-read"
	checksum := crc32.ChecksumIEEE(testData)

	blockFilePath := filepath.Join(tmpDir, fmt.Sprintf("%s.bin", blockID))
	err := os.WriteFile(blockFilePath, testData, 0644)
	require.NoError(t, err)

	checksumFilePath := filepath.Join(tmpDir, fmt.Sprintf("%s.checksum", blockID))
	err = os.WriteFile(checksumFilePath, []byte(fmt.Sprintf("%d", checksum)), 0644)
	require.NoError(t, err)

	err = worker.verifyBlockIntegrity(blockID)
	assert.NoError(t, err, "Integrity check should pass for valid block")
}

func TestChecksumDetectsCorruption(t *testing.T) {
	tmpDir := t.TempDir()
	worker := NewWorkerNode(tmpDir, 50051)

	originalData := []byte("Original data")
	blockID := "test-block-corrupt"
	originalChecksum := crc32.ChecksumIEEE(originalData)

	blockFilePath := filepath.Join(tmpDir, fmt.Sprintf("%s.bin", blockID))
	err := os.WriteFile(blockFilePath, originalData, 0644)
	require.NoError(t, err)

	checksumFilePath := filepath.Join(tmpDir, fmt.Sprintf("%s.checksum", blockID))
	err = os.WriteFile(checksumFilePath, []byte(fmt.Sprintf("%d", originalChecksum)), 0644)
	require.NoError(t, err)

	corruptedData := []byte("Corrupted data - different!")
	err = os.WriteFile(blockFilePath, corruptedData, 0644)
	require.NoError(t, err)

	err = worker.verifyBlockIntegrity(blockID)
	assert.Error(t, err, "Integrity check should fail for corrupted block")
	assert.Contains(t, err.Error(), "checksum mismatch", "Error should indicate checksum mismatch")
	assert.Contains(t, err.Error(), "CORRUPTION DETECTED", "Error should indicate corruption")
}

func TestGetBlockChecksum(t *testing.T) {
	tmpDir := t.TempDir()
	worker := NewWorkerNode(tmpDir, 50051)

	blockID := "test-block-checksum-api"
	expectedChecksum := uint32(12345678)

	checksumFilePath := filepath.Join(tmpDir, fmt.Sprintf("%s.checksum", blockID))
	err := os.WriteFile(checksumFilePath, []byte(fmt.Sprintf("%d", expectedChecksum)), 0644)
	require.NoError(t, err)

	resp, err := worker.GetBlockChecksum(context.Background(), &datanodev1.GetBlockChecksumRequest{
		BlockId: blockID,
	})
	require.NoError(t, err)
	assert.True(t, resp.Exists)
	assert.Equal(t, expectedChecksum, resp.Checksum)
}

func TestGetBlockChecksumNotFound(t *testing.T) {
	tmpDir := t.TempDir()
	worker := NewWorkerNode(tmpDir, 50051)

	resp, err := worker.GetBlockChecksum(context.Background(), &datanodev1.GetBlockChecksumRequest{
		BlockId: "non-existent-block",
	})
	require.NoError(t, err)
	assert.False(t, resp.Exists)
	assert.Equal(t, uint32(0), resp.Checksum)
}

func TestDeleteBlockRemovesChecksumFile(t *testing.T) {
	tmpDir := t.TempDir()
	worker := NewWorkerNode(tmpDir, 50051)

	blockID := "test-block-delete"
	testData := []byte("Data to be deleted")
	checksum := crc32.ChecksumIEEE(testData)

	blockFilePath := filepath.Join(tmpDir, fmt.Sprintf("%s.bin", blockID))
	err := os.WriteFile(blockFilePath, testData, 0644)
	require.NoError(t, err)

	checksumFilePath := filepath.Join(tmpDir, fmt.Sprintf("%s.checksum", blockID))
	err = os.WriteFile(checksumFilePath, []byte(fmt.Sprintf("%d", checksum)), 0644)
	require.NoError(t, err)

	resp, err := worker.DeleteBlock(context.Background(), &datanodev1.DeleteBlockRequest{
		BlockId: blockID,
	})
	require.NoError(t, err)
	assert.True(t, resp.Success)

	assert.NoFileExists(t, blockFilePath)
	assert.NoFileExists(t, checksumFilePath)
}
