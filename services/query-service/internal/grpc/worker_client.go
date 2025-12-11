package grpc

import (
	"context"

	"github.com/razvanmarinn/datalake/protobuf"
	"google.golang.org/grpc"
)

// WorkerClient is a client for the WorkerService.
type WorkerClient struct {
	conn    *grpc.ClientConn
	service protobuf.BatchReceiverServiceClient
}

// NewWorkerClient creates a new WorkerClient.
func NewWorkerClient(address string) (*WorkerClient, error) {
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	return &WorkerClient{
		conn:    conn,
		service: protobuf.NewBatchReceiverServiceClient(conn),
	}, nil
}

// Close closes the client connection.
func (c *WorkerClient) Close() {
	c.conn.Close()
}

// RetrieveBatch retrieves a batch from a worker.
func (c *WorkerClient) RetrieveBatch(ctx context.Context, batchID string) (*protobuf.WorkerBatchResponse, error) {
	req := &protobuf.GetClientRequestToWorker{
		BatchId: batchID,
	}
	return c.service.RetrieveBatchForClient(ctx, req)
}
