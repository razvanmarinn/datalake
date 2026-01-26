package dfs

import (
	"context"
	"fmt"
	"io"

	coordinatorv1 "github.com/razvanmarinn/datalake/protobuf/gen/go/coordinator/v1"
	datanodev1 "github.com/razvanmarinn/datalake/protobuf/gen/go/datanode/v1"
)

type reader struct {
	client   *dfsClient
	ctx      context.Context
	path     string
	fileSize int64
	offset   int64
	metadata *coordinatorv1.GetFileMetadataResponse
}

func (c *dfsClient) Open(ctx context.Context, path string) (File, error) {
	resp, err := c.masterClient.GetFileMetadata(ctx, &coordinatorv1.GetFileMetadataRequest{
		ProjectId: "default",
		FilePath:  path,
	})
	if err != nil {
		return nil, err
	}

	var totalSize int64
	for _, b := range resp.Blocks {
		totalSize += b.Size
	}

	return &reader{
		client:   c,
		ctx:      ctx,
		path:     path,
		metadata: resp,
		fileSize: totalSize,
		offset:   0,
	}, nil
}

func (r *reader) Read(p []byte) (n int, err error) {
	if r.offset >= r.fileSize {
		return 0, io.EOF
	}

	blockIdx, blockOffset, err := r.locateBlock(r.offset)
	if err != nil {
		return 0, err
	}

	blockInfo := r.metadata.Blocks[blockIdx]
	bytesLeftInBlock := blockInfo.Size - blockOffset
	toRead := int64(len(p))
	if toRead > bytesLeftInBlock {
		toRead = bytesLeftInBlock
	}

	loc, ok := r.metadata.Locations[blockInfo.BlockId]
	if !ok {
		return 0, fmt.Errorf("no location for block %s", blockInfo.BlockId)
	}

	workerClient, err := r.client.getWorkerClient(loc.Address)
	if err != nil {
		return 0, err
	}

	stream, err := workerClient.FetchBlock(r.ctx, &datanodev1.FetchBlockRequest{
		BlockId: blockInfo.BlockId,
	})
	if err != nil {
		return 0, err
	}

	var discarded int64
	bytesRead := 0
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return bytesRead, err
		}

		chunk := resp.Chunk
		if discarded < blockOffset {
			if discarded+int64(len(chunk)) <= blockOffset {
				discarded += int64(len(chunk))
				continue
			}
			chunk = chunk[blockOffset-discarded:]
			discarded = blockOffset
		}

		take := int64(len(chunk))
		if int64(bytesRead)+take > toRead {
			take = toRead - int64(bytesRead)
		}

		copy(p[bytesRead:], chunk[:take])
		bytesRead += int(take)

		if int64(bytesRead) >= toRead {
			break
		}
	}

	r.offset += int64(bytesRead)
	return bytesRead, nil
}

func (r *reader) locateBlock(globalOffset int64) (index int, offsetInBlock int64, err error) {
	var currentPos int64 = 0
	for i, block := range r.metadata.Blocks {
		if globalOffset >= currentPos && globalOffset < currentPos+block.Size {
			return i, globalOffset - currentPos, nil
		}
		currentPos += block.Size
	}
	return -1, 0, io.EOF
}

func (r *reader) Write(p []byte) (n int, err error) {
	return 0, fmt.Errorf("read-only")
}

func (r *reader) Close() error {
	return nil
}

func (r *reader) Sync() error {
	return nil
}

func (r *reader) Stat() (FileInfo, error) {
	info := FileInfo{
		Name: r.path,
		Size: r.fileSize,
	}
	for _, b := range r.metadata.Blocks {
		loc := r.metadata.Locations[b.BlockId]
		info.Blocks = append(info.Blocks, BlockMetadata{
			BlockId:  b.BlockId,
			Size:     b.Size,
			WorkerId: loc.WorkerId,
			Address:  loc.Address,
		})
	}
	return info, nil
}

func (r *reader) Seek(offset int64, whence int) (int64, error) {
	var newOffset int64
	switch whence {
	case io.SeekStart:
		newOffset = offset
	case io.SeekCurrent:
		newOffset = r.offset + offset
	case io.SeekEnd:
		newOffset = r.fileSize + offset
	default:
		return 0, fmt.Errorf("invalid whence")
	}

	if newOffset < 0 || newOffset > r.fileSize {
		return 0, fmt.Errorf("offset out of bounds")
	}

	r.offset = newOffset
	return r.offset, nil
}
