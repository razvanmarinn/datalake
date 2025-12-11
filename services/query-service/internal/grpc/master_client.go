package grpc

import (
	"context"
	"log"

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
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
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
		OnwerId: owner_id,
		ProjectId: project_id
	}
	return c.service.GetFileListForProject(ctx, req)

}
