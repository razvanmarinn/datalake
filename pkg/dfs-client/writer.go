package dfs

import (
	"bytes"
	"context"
	"fmt"
	"io"

	commonv1 "github.com/razvanmarinn/datalake/protobuf/gen/go/common/v1"
	coordinatorv1 "github.com/razvanmarinn/datalake/protobuf/gen/go/coordinator/v1"
	datanodev1 "github.com/razvanmarinn/datalake/protobuf/gen/go/datanode/v1"
)

const (
	DefaultBlockSize = 64 * 1024 * 1024
	streamChunkSize  = 2 * 1024 * 1024
)

type writer struct {
	client        *dfsClient
	ctx           context.Context
	path          string
	projectID     string
	ownerID       string
	format        string
	currentBuffer *bytes.Buffer
	writtenBlocks []BlockMetadata
}

type CreateOption func(*writer)

func WithProjectID(pid string) CreateOption {
	return func(w *writer) { w.projectID = pid }
}

func WithOwnerID(oid string) CreateOption {
	return func(w *writer) { w.ownerID = oid }
}

func WithFormat(fmt string) CreateOption {
	return func(w *writer) { w.format = fmt }
}

func (c *dfsClient) Create(ctx context.Context, path string, opts ...CreateOption) (File, error) {
	w := &writer{
		client:        c,
		ctx:           ctx,
		path:          path,
		currentBuffer: bytes.NewBuffer(make([]byte, 0, DefaultBlockSize)),
		format:        "bin",
		projectID:     "default",
	}

	for _, opt := range opts {
		opt(w)
	}

	return w, nil
}

func (w *writer) Write(p []byte) (n int, err error) {
	totalWritten := 0
	for len(p) > 0 {
		available := DefaultBlockSize - w.currentBuffer.Len()
		if available <= 0 {
			if err := w.flushBlock(); err != nil {
				return totalWritten, err
			}
			available = DefaultBlockSize
		}

		toWrite := len(p)
		if toWrite > available {
			toWrite = available
		}

		n, _ := w.currentBuffer.Write(p[:toWrite])
		totalWritten += n
		p = p[toWrite:]
	}
	return totalWritten, nil
}

func (w *writer) flushBlock() error {
	if w.currentBuffer.Len() == 0 {
		return nil
	}

	dataSize := int64(w.currentBuffer.Len())
	allocResp, err := w.client.masterClient.AllocateBlock(w.ctx, &coordinatorv1.AllocateBlockRequest{
		ProjectId: w.projectID,
		SizeBytes: dataSize,
	})
	if err != nil {
		return err
	}

	if len(allocResp.TargetDatanodes) == 0 {
		return fmt.Errorf("no targets")
	}
	target := allocResp.TargetDatanodes[0]

	workerClient, err := w.client.getWorkerClient(target.Address)
	if err != nil {
		return err
	}

	stream, err := workerClient.PushBlock(w.ctx)
	if err != nil {
		return err
	}

	err = stream.Send(&datanodev1.PushBlockRequest{
		Data: &datanodev1.PushBlockRequest_Metadata{
			Metadata: &datanodev1.BlockMetadata{
				BlockId:   allocResp.BlockId,
				TotalSize: dataSize,
			},
		},
	})
	if err != nil {
		return err
	}

	rawBytes := w.currentBuffer.Bytes()
	for i := 0; i < len(rawBytes); i += streamChunkSize {
		end := i + streamChunkSize
		if end > len(rawBytes) {
			end = len(rawBytes)
		}
		err = stream.Send(&datanodev1.PushBlockRequest{
			Data: &datanodev1.PushBlockRequest_Chunk{
				Chunk: rawBytes[i:end],
			},
		})
		if err != nil {
			return err
		}
	}

	resp, err := stream.CloseAndRecv()
	if err != nil {
		return err
	}
	if !resp.Success {
		return fmt.Errorf("%s", resp.Message)
	}

	w.writtenBlocks = append(w.writtenBlocks, BlockMetadata{
		BlockId:  allocResp.BlockId,
		Size:     dataSize,
		WorkerId: target.WorkerId,
		Address:  target.Address,
	})

	w.currentBuffer.Reset()
	return nil
}

func (w *writer) Close() error {
	if err := w.flushBlock(); err != nil {
		return err
	}

	protoBlocks := make([]*commonv1.BlockInfo, len(w.writtenBlocks))
	for i, b := range w.writtenBlocks {
		protoBlocks[i] = &commonv1.BlockInfo{
			BlockId: b.BlockId,
			Size:    b.Size,
		}
	}

	_, err := w.client.masterClient.CommitFile(w.ctx, &coordinatorv1.CommitFileRequest{
		ProjectId:  w.projectID,
		OwnerId:    w.ownerID,
		FilePath:   w.path,
		FileFormat: w.format,
		Blocks:     protoBlocks,
	})
	return err
}

func (w *writer) Read(p []byte) (n int, err error) {
	return 0, io.EOF
}

func (w *writer) Sync() error {
	return w.flushBlock()
}

func (w *writer) Stat() (FileInfo, error) {
	var total int64
	for _, b := range w.writtenBlocks {
		total += b.Size
	}
	return FileInfo{
		Name:   w.path,
		Size:   total + int64(w.currentBuffer.Len()),
		Blocks: w.writtenBlocks,
	}, nil
}

func (w *writer) Seek(offset int64, whence int) (int64, error) {
	return 0, fmt.Errorf("not supported")
}
