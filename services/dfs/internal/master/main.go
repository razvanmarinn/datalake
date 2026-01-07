package main

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/razvanmarinn/datalake/pkg/logging"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	coordinatorv1 "github.com/razvanmarinn/datalake/protobuf/gen/go/coordinator/v1"
	"github.com/razvanmarinn/dfs/internal/nodes"
)

const (
	port = ":50055"
)

type server struct {
	coordinatorv1.UnimplementedCoordinatorServiceServer
	masterNode *nodes.MasterNode
	logger     *logging.Logger
}

func (s *server) AllocateBlock(ctx context.Context, req *coordinatorv1.AllocateBlockRequest) (*coordinatorv1.AllocateBlockResponse, error) {
	s.logger.Info("Received AllocateBlock request",
		zap.String("project_id", req.ProjectId),
		zap.Int64("size", req.SizeBytes))

	resp, err := s.masterNode.AllocateBlock(req)
	if err != nil {
		s.logger.Error("Allocation failed", zap.Error(err))
		return nil, err
	}
	return resp, nil
}

// 2. CommitFile
func (s *server) CommitFile(ctx context.Context, req *coordinatorv1.CommitFileRequest) (*coordinatorv1.CommitFileResponse, error) {
	s.logger.Info("Received CommitFile request", zap.String("file_path", req.FilePath))

	_, err := s.masterNode.CommitFile(req)
	if err != nil {
		s.logger.Error("Commit failed", zap.Error(err))
		return &coordinatorv1.CommitFileResponse{Success: false}, err
	}

	return &coordinatorv1.CommitFileResponse{Success: true}, nil
}

// 3. GetFileMetadata
func (s *server) GetFileMetadata(ctx context.Context, req *coordinatorv1.GetFileMetadataRequest) (*coordinatorv1.GetFileMetadataResponse, error) {
	s.logger.Info("Received GetFileMetadata request", zap.String("file_path", req.FilePath))

	resp, err := s.masterNode.GetFileMetadata(req.ProjectId, req.FilePath)
	if err != nil {
		s.logger.Error("Metadata retrieval failed", zap.Error(err))
		return nil, err
	}
	return resp, nil
}

// 4. ListFiles
func (s *server) ListFiles(ctx context.Context, req *coordinatorv1.ListFilesRequest) (*coordinatorv1.ListFilesResponse, error) {
	files := s.masterNode.ListFiles(req.ProjectId, req.DirectoryPrefix)
	return &coordinatorv1.ListFilesResponse{FilePaths: files}, nil
}

// 5. CommitCompaction (Stub)
func (s *server) CommitCompaction(ctx context.Context, req *coordinatorv1.CommitCompactionRequest) (*coordinatorv1.CommitCompactionResponse, error) {
	return &coordinatorv1.CommitCompactionResponse{Success: true}, nil
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	logger := logging.NewDefaultLogger("master_node")
	defer logger.Sync()

	// Initialize State and MasterNode
	state := nodes.NewMasterNodeState()
	_ = state.LoadStateFromFile() // Handle error properly in prod

	masterNode := nodes.GetMasterNodeInstance()

	// IMPORTANT: Initialize LoadBalancer before starting server
	if err := masterNode.InitializeLoadBalancer(3, 50051); err != nil {
		logger.Fatal("Failed to init load balancer", zap.Error(err))
	}
	defer masterNode.CloseLoadBalancer()

	lis, err := net.Listen("tcp", port)
	if err != nil {
		logger.Fatal("Failed to listen", zap.Error(err))
	}

	s := grpc.NewServer()

	// Register the Coordinator Service
	coordinatorv1.RegisterCoordinatorServiceServer(s, &server{
		masterNode: masterNode,
		logger:     logger,
	})

	logger.Info("Coordinator Service listening", zap.String("address", port))

	// Graceful Shutdown Logic
	go func() {
		sigs := make(chan os.Signal, 1)
		signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
		<-sigs
		logger.Info("Shutting down...")
		s.GracefulStop()
		masterNode.Stop()
	}()

	if err := s.Serve(lis); err != nil {
		logger.Fatal("Server failed", zap.Error(err))
	}
}
