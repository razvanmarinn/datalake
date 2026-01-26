package dfs

import (
	"context"
	"fmt"
	"io"
	"sync"

	coordinatorv1 "github.com/razvanmarinn/datalake/protobuf/gen/go/coordinator/v1"
	datanodev1 "github.com/razvanmarinn/datalake/protobuf/gen/go/datanode/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Client interface {
	Create(ctx context.Context, path string, opts ...CreateOption) (File, error)
	Open(ctx context.Context, path string) (File, error)
	Delete(ctx context.Context, path string) error
	Rename(ctx context.Context, oldPath, newPath string) error
	MkdirAll(ctx context.Context, path string) error
	List(ctx context.Context, path string) ([]string, error)
	Close() error
}

type File interface {
	io.Reader
	io.Writer
	io.Seeker
	io.Closer
	Sync() error
	Stat() (FileInfo, error)
}

type FileInfo struct {
	Name    string
	Size    int64
	IsDir   bool
	ModTime int64
	Blocks  []BlockMetadata
}

type BlockMetadata struct {
	BlockId  string
	Size     int64
	WorkerId string
	Address  string
}

type dfsClient struct {
	masterURL    string
	masterConn   *grpc.ClientConn
	masterClient coordinatorv1.CoordinatorServiceClient
	workerConns  sync.Map
}

func NewClient(masterAddr string) (Client, error) {
	conn, err := grpc.NewClient(masterAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}

	return &dfsClient{
		masterURL:    masterAddr,
		masterConn:   conn,
		masterClient: coordinatorv1.NewCoordinatorServiceClient(conn),
	}, nil
}

func (c *dfsClient) getWorkerClient(addr string) (datanodev1.DataNodeServiceClient, error) {
	if conn, ok := c.workerConns.Load(addr); ok {
		return datanodev1.NewDataNodeServiceClient(conn.(*grpc.ClientConn)), nil
	}

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}

	actual, loaded := c.workerConns.LoadOrStore(addr, conn)
	if loaded {
		conn.Close()
	}

	return datanodev1.NewDataNodeServiceClient(actual.(*grpc.ClientConn)), nil
}

func (c *dfsClient) List(ctx context.Context, path string) ([]string, error) {
	resp, err := c.masterClient.ListFiles(ctx, &coordinatorv1.ListFilesRequest{
		ProjectId:       "default",
		DirectoryPrefix: path,
	})
	if err != nil {
		return nil, err
	}
	return resp.FilePaths, nil
}

func (c *dfsClient) Delete(ctx context.Context, path string) error {
	return fmt.Errorf("not implemented")
}

func (c *dfsClient) Rename(ctx context.Context, oldPath, newPath string) error {
	return fmt.Errorf("not implemented")
}

func (c *dfsClient) MkdirAll(ctx context.Context, path string) error {
	return nil
}

func (c *dfsClient) Close() error {
	var err error
	if c.masterConn != nil {
		err = c.masterConn.Close()
	}

	c.workerConns.Range(func(key, value any) bool {
		if conn, ok := value.(*grpc.ClientConn); ok {
			conn.Close()
		}
		return true
	})

	return err
}
