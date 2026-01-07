package grpc

import (
	"bytes"
	"context"
	"fmt"
	"io"

	datanodev1 "github.com/razvanmarinn/datalake/protobuf/gen/go/datanode/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// DataNodeClient is a client for the DataNodeService.
type DataNodeClient struct {
	conn    *grpc.ClientConn
	service datanodev1.DataNodeServiceClient
}

// NewDataNodeClient creates a new DataNodeClient.
func NewDataNodeClient(address string) (*DataNodeClient, error) {
	conn, err := grpc.Dial(
		address, 
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, err
	}
	return &DataNodeClient{
		conn:    conn,
		service: datanodev1.NewDataNodeServiceClient(conn),
	}, nil
}

// Close closes the client connection.
func (c *DataNodeClient) Close() {
	c.conn.Close()
}

// FetchBlock retrieves a complete block from the DataNode by consuming the stream.
func (c *DataNodeClient) FetchBlock(ctx context.Context, blockID string) ([]byte, error) {
	req := &datanodev1.FetchBlockRequest{
		BlockId: blockID,
	}

	stream, err := c.service.FetchBlock(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("failed to start fetch stream: %w", err)
	}

	var dataBuffer bytes.Buffer

	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("error receiving chunk: %w", err)
		}
		
		// Write the received chunk to the buffer
		if _, err := dataBuffer.Write(resp.Chunk); err != nil {
			return nil, fmt.Errorf("failed to write chunk to buffer: %w", err)
		}
	}

	return dataBuffer.Bytes(), nil
}

// GetWorkerInfo retrieves info about the datanode (optional helper).
func (c *DataNodeClient) GetWorkerInfo(ctx context.Context) (*datanodev1.GetWorkerInfoResponse, error) {
	return c.service.GetWorkerInfo(ctx, &datanodev1.GetWorkerInfoRequest{})
}