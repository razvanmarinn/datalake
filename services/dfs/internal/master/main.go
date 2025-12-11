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
	fm := s.masterNode.RegisterFile(in)

	s.masterNode.FileRegistry = append(s.masterNode.FileRegistry, *fm)
	return &pb.MasterFileResponse{Success: true}, nil
}

func (s *server) GetBatchDestination(ctx context.Context, in *pb.ClientBatchRequestToMaster) (*pb.MasterResponse, error) {
	s.logger.Info("Received GetBatchDestination request for batch: %v", zap.String("batch_id", in.GetBatchId()))

	wid, wm := s.masterNode.LoadBalancer.GetNextClient()
	batchuuid, err := uuid.Parse(in.GetBatchId())
	if err != nil {
		s.logger.Error("Invalid batch ID format", zap.String("batch_id", in.GetBatchId()), zap.Error(err))
	}
	s.masterNode.UpdateBatchLocation(batchuuid, wid)

	return &pb.MasterResponse{WorkerIp: wm.Ip, WorkerPort: wm.Port}, nil
}

func (s *server) GetMetadata(ctx context.Context, in *pb.Location) (*pb.MasterMetadataResponse, error) {
	s.logger.Info("Received GetMetadata request for file: %v", zap.String("file_name", in.GetFileName()))

	uuids := s.masterNode.GetFileBatches(in.GetFileName())
	batch_ids := make([]string, len(uuids))
	for i, id := range uuids {
		batch_ids[i] = id.String()
	}

	batchLocations := make(map[string]*pb.BatchLocation)

	for _, bId := range uuids {
		worker_node_ids := s.masterNode.GetBatchLocations(bId)

		workerAddresses := make([]string, 0)

		for _, wId := range worker_node_ids {
			_, ip, port, err := s.masterNode.LoadBalancer.GetClientByWorkerID(wId.String())
			if err != nil {
				s.logger.Error("Error getting client for worker ID", zap.String("worker_id", wId.String()), zap.Error(err))
				continue
			}
			combineIpAndPort := func(ip string, port int32) string {
				return fmt.Sprintf("%s:%d", ip, port)

			}
			address := combineIpAndPort(ip, port)
			workerAddresses = append(workerAddresses, address)
		}
		batchLocations[bId.String()] = &pb.BatchLocation{
			WorkerIds: workerAddresses,
		}
	}

	return &pb.MasterMetadataResponse{
		BatchIds:       batch_ids,
		BatchLocations: batchLocations,
	}, nil
}

func (s *server) GetFileListForProject(ctx context.Context, in *pb.ApiRequestForFileList) (*pb.FileListResponse, error) {
	log.Printf("GetFileListForProject called with OwnerID=%s, ProjectID=%s", in.OwnerId, in.ProjectId)

	// Log the full FileRegistry
	log.Printf("Current FileRegistry: %+v", s.masterNode.FileRegistry)

	fileList := make([]string, 0)

	for _, file := range s.masterNode.FileRegistry {
		log.Printf("Checking file: Name=%s, OwnerID=%s, ProjectID=%s", file.Name, file.OwnerID, file.ProjectID)

		if file.OwnerID == in.OwnerId && file.ProjectID == in.ProjectId {
			log.Printf("Adding file to response: %s", file.Name)
			fileList = append(fileList, file.Name)
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
