package grpc

import (
	"context"
	"fmt"
	"log"
	"time"

	coordinatorv1 "github.com/razvanmarinn/datalake/protobuf/gen/go/coordinator/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// MasterClient is a client for the CoordinatorService.
type MasterClient struct {
	conn    *grpc.ClientConn
	service coordinatorv1.CoordinatorServiceClient
}

// NewMasterClient creates a new MasterClient.
func NewMasterClient(address string) (*MasterClient, error) {
	log.Printf("Attempting to dial Coordinator at address: %s", address)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Updated to use google.golang.org/grpc/credentials/insecure
	conn, err := grpc.DialContext(
		ctx,
		address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)

	if err != nil {
		log.Printf("FATAL ERROR: Failed to connect to Coordinator at %s: %v", address, err)
		return nil, fmt.Errorf("could not connect to coordinator at %s: %w", address, err)
	}

	log.Printf("Successfully connected to Coordinator at: %s", address)

	return &MasterClient{
		conn:    conn,
		service: coordinatorv1.NewCoordinatorServiceClient(conn),
	}, nil
}

// Close closes the client connection.
func (c *MasterClient) Close() {
	c.conn.Close()
}

// GetFileMetadata calls the GetFileMetadata RPC.
func (c *MasterClient) GetFileMetadata(ctx context.Context, projectID, filePath string) (*coordinatorv1.GetFileMetadataResponse, error) {
	log.Printf("Querying coordinator for metadata: project=%s file=%s", projectID, filePath)

	req := &coordinatorv1.GetFileMetadataRequest{
		ProjectId: projectID,
		FilePath:  filePath,
	}
	return c.service.GetFileMetadata(ctx, req)
}

// ListFiles calls the ListFiles RPC.
func (c *MasterClient) ListFiles(ctx context.Context, projectID, directoryPrefix string) (*coordinatorv1.ListFilesResponse, error) {
	log.Printf("Listing files: project=%s prefix=%s", projectID, directoryPrefix)

	req := &coordinatorv1.ListFilesRequest{
		ProjectId:       projectID,
		DirectoryPrefix: directoryPrefix,
	}
	return c.service.ListFiles(ctx, req)
}
