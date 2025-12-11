package grpc

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/razvanmarinn/datalake/protobuf"
	"google.golang.org/grpc"
)

// MasterClient is a client for the MasterService.
type MasterClient struct {
	conn    *grpc.ClientConn
	service protobuf.MasterServiceClient
}

// NewMasterClient creates a new MasterClient.
func NewMasterClient(address string) (*MasterClient, error) {
	// 1. LOG THE INPUT ADDRESS
	log.Printf("Attempting to dial Master Node at address: %s", address)

	// Using grpc.WithBlock() ensures the client waits for the connection
	// to establish and returns an error if it cannot connect within the timeout.
	// We add a 5-second timeout context for robust connection attempts.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(
		ctx,
		address,
		grpc.WithInsecure(),
		grpc.WithBlock(), // Wait until the connection is established or fails
	)

	if err != nil {
		// 2. LOG THE CONNECTION ERROR
		log.Printf("FATAL ERROR: Failed to connect to Master Node at %s: %v", address, err)
		return nil, fmt.Errorf("could not connect to master node at %s: %w", address, err)
	}

	// 3. LOG SUCCESS
	log.Printf("Successfully connected to Master Node at: %s", address)

	return &MasterClient{
		conn:    conn,
		service: protobuf.NewMasterServiceClient(conn),
	}, nil
}

// Close closes the client connection.
func (c *MasterClient) Close() {
	c.conn.Close()
}

// GetMetadata calls the GetMetadata RPC on the MasterService.
func (c *MasterClient) GetMetadata(ctx context.Context, fileName string) (*protobuf.MasterMetadataResponse, error) {
	log.Printf("Querying master for metadata for file: %s", fileName)
	req := &protobuf.Location{
		FileName: fileName,
	}
	return c.service.GetMetadata(ctx, req)
}

// rpc GetFileListForProject(common.ApiRequestForFileList) returns (common.FileListResponse) {}
func (c *MasterClient) GetFileListForProject(ctx context.Context, owner_id string, project_id string) (*protobuf.FileListResponse, error) {
	req := &protobuf.ApiRequestForFileList{
		OwnerId:   owner_id,
		ProjectId: project_id,
	}
	return c.service.GetFileListForProject(ctx, req)

}
