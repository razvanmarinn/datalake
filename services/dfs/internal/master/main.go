package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/razvanmarinn/datalake/pkg/logging"
	"go.uber.org/zap"

	"github.com/google/uuid"
	pb "github.com/razvanmarinn/datalake/protobuf"
	"github.com/razvanmarinn/dfs/internal/nodes"

	"google.golang.org/grpc"
)

const (
	port      = ":50055"
	stateFile = "master_node_state.json"
)

type server struct {
	pb.UnimplementedMasterServiceServer
	masterNode *nodes.MasterNode
	logger     *logging.Logger
}

func (s *server) RegisterFile(ctx context.Context, in *pb.ClientFileRequestToMaster) (*pb.MasterFileResponse, error) {
	inode := s.masterNode.RegisterFile(in)
	
	s.logger.Info("File registered successfully", 
		zap.String("file_name", inode.Name), 
		zap.String("file_id", inode.ID))

	return &pb.MasterFileResponse{Success: true}, nil
}

func (s *server) GetBatchDestination(ctx context.Context, in *pb.ClientBatchRequestToMaster) (*pb.MasterResponse, error) {
	s.logger.Info("Received GetBatchDestination request for batch", zap.String("batch_id", in.GetBatchId()))

	wid, wm := s.masterNode.LoadBalancer.GetNextClient()
	
	blockUUID, err := uuid.Parse(in.GetBatchId())
	if err != nil {
		s.logger.Error("Invalid batch ID format", zap.String("batch_id", in.GetBatchId()), zap.Error(err))
		return nil, err
	}

	// This updates the BlockMap in the master node
	s.masterNode.UpdateBatchLocation(blockUUID, wid)

	return &pb.MasterResponse{WorkerIp: wm.Ip, WorkerPort: wm.Port}, nil
}

func (s *server) GetMetadata(ctx context.Context, in *pb.Location) (*pb.MasterMetadataResponse, error) {
	s.logger.Info("Received GetMetadata request for file", zap.String("file_name", in.GetFileName()))

	blockUUIDs := s.masterNode.GetFileBatches(in.GetFileName())
	if blockUUIDs == nil {
		return &pb.MasterMetadataResponse{}, fmt.Errorf("file not found: %s", in.GetFileName())
	}

	batchIds := make([]string, len(blockUUIDs))
	for i, id := range blockUUIDs {
		batchIds[i] = id.String()
	}

	batchLocations := make(map[string]*pb.BatchLocation)

	for _, bId := range blockUUIDs {
		workerNodeUUIDs := s.masterNode.GetBatchLocations(bId)
		workerAddresses := make([]string, 0)

		for _, wId := range workerNodeUUIDs {
			_, ip, port, err := s.masterNode.LoadBalancer.GetClientByWorkerID(wId.String())
			if err != nil {
				s.logger.Error("Error getting client for worker ID", zap.String("worker_id", wId.String()), zap.Error(err))
				continue
			}
			address := fmt.Sprintf("%s:%d", ip, port)
			workerAddresses = append(workerAddresses, address)
		}
		
		batchLocations[bId.String()] = &pb.BatchLocation{
			WorkerIds: workerAddresses,
		}
	}

	return &pb.MasterMetadataResponse{
		BatchIds:       batchIds,
		BatchLocations: batchLocations,
	}, nil
}

func (s *server) GetFileListForProject(ctx context.Context, in *pb.ApiRequestForFileList) (*pb.FileListResponse, error) {
	log.Printf("GetFileListForProject called with OwnerID=%s, ProjectID=%s", in.OwnerId, in.ProjectId)

	s.masterNode.Lock() // Assuming you exported the lock or added a Lock/Unlock method. 

	fileList := make([]string, 0)


	for _, inode := range s.masterNode.Namespace {
		if inode.OwnerID == in.OwnerId && inode.ProjectID == in.ProjectId {
			fileList = append(fileList, inode.Name)
		}
	}

	log.Printf("Total files found: %d", len(fileList))
	return &pb.FileListResponse{FileListNames: fileList}, nil
}



func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	log.SetOutput(os.Stdout)

	logger := logging.NewDefaultLogger("master_node")
	defer logger.Sync()
	
	state := nodes.NewMasterNodeState()
	masterNode := nodes.GetMasterNodeInstance()

	masterNode.InitializeLoadBalancer(3, 50051)
	defer masterNode.CloseLoadBalancer()

	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	s := grpc.NewServer()
	pb.RegisterMasterServiceServer(s, &server{
		masterNode: masterNode,
		logger:     logger,
	})

	logger.Info("gRPC server listening", zap.String("address", lis.Addr().String()))

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		if err := s.Serve(lis); err != nil {
			logger.Error("gRPC server failed", zap.Error(err))
		}
	}()

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigs
		logger.Info("Received signal, shutting down", zap.String("signal", sig.String()))
		logger.Info("Shutting down master node and gRPC server")

		state.UpdateState(masterNode)
		
		if err := state.SaveState(); err != nil {
			logger.Error("Failed to save master node state", zap.Error(err))
		}

		s.GracefulStop()
		masterNode.Stop()
		logger.Info("Master node and gRPC server stopped")
		wg.Done()
	}()

	logger.Info("Master node and gRPC server are running. Press Ctrl+C to stop.")
	wg.Wait()
	logger.Info("Main function exiting")
}