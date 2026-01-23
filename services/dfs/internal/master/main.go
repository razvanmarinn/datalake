package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/razvanmarinn/datalake/pkg/logging"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"

	coordinatorv1 "github.com/razvanmarinn/datalake/protobuf/gen/go/coordinator/v1"
	replicationv1 "github.com/razvanmarinn/datalake/protobuf/gen/go/replication/v1"
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
	if !s.masterNode.IsActive {
		return nil, fmt.Errorf("node is standby, not active leader")
	}

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

func (s *server) CommitFile(ctx context.Context, req *coordinatorv1.CommitFileRequest) (*coordinatorv1.CommitFileResponse, error) {
	if !s.masterNode.IsActive {
		return &coordinatorv1.CommitFileResponse{Success: false}, fmt.Errorf("node is standby")
	}
	s.logger.Info("Received CommitFile request", zap.String("file_path", req.FilePath))

	_, err := s.masterNode.CommitFile(req)
	if err != nil {
		s.logger.Error("Commit failed", zap.Error(err))
		return &coordinatorv1.CommitFileResponse{Success: false}, err
	}

	return &coordinatorv1.CommitFileResponse{Success: true}, nil
}

func (s *server) GetFileMetadata(ctx context.Context, req *coordinatorv1.GetFileMetadataRequest) (*coordinatorv1.GetFileMetadataResponse, error) {
	if !s.masterNode.IsActive {
		return nil, fmt.Errorf("node is standby")
	}
	s.logger.Info("Received GetFileMetadata request", zap.String("file_path", req.FilePath))

	resp, err := s.masterNode.GetFileMetadata(req.ProjectId, req.FilePath)
	if err != nil {
		s.logger.Error("Metadata retrieval failed", zap.Error(err))
		return nil, err
	}
	return resp, nil
}

func (s *server) ListFiles(ctx context.Context, req *coordinatorv1.ListFilesRequest) (*coordinatorv1.ListFilesResponse, error) {
	if !s.masterNode.IsActive {
		return nil, fmt.Errorf("node is standby")
	}
	files, err := s.masterNode.ListFiles(req.ProjectId, req.DirectoryPrefix)
	if err != nil {
		s.logger.Error("ListFiles failed", zap.Error(err))
		return nil, err
	}
	return &coordinatorv1.ListFilesResponse{FilePaths: files}, nil
}

func (s *server) CommitCompaction(ctx context.Context, req *coordinatorv1.CommitCompactionRequest) (*coordinatorv1.CommitCompactionResponse, error) {
	if !s.masterNode.IsActive {
		return &coordinatorv1.CommitCompactionResponse{Success: false}, fmt.Errorf("node is standby")
	}

	s.logger.Info("Received CommitCompaction request",
		zap.String("project_id", req.ProjectId),
		zap.Int("old_files_count", len(req.OldFilePaths)))

	err := s.masterNode.CommitCompaction(req)
	if err != nil {
		s.logger.Error("Compaction Commit failed", zap.Error(err))
		return &coordinatorv1.CommitCompactionResponse{Success: false}, err
	}

	return &coordinatorv1.CommitCompactionResponse{Success: true}, nil
}

type replicationServer struct {
	replicationv1.UnimplementedReplicationServiceServer
	masterNode *nodes.MasterNode
	logger     *logging.Logger
}

func (s *replicationServer) ReplicateLog(ctx context.Context, req *replicationv1.ReplicateLogRequest) (*replicationv1.ReplicateLogResponse, error) {
	if s.masterNode.IsActive {
		return &replicationv1.ReplicateLogResponse{Success: false}, fmt.Errorf("I am leader, cannot follow")
	}

	s.logger.Debug("Received Replication Log", zap.Int32("op_type", req.OpType))

	err := s.masterNode.ApplyReplicatedLog(nodes.OpType(req.OpType), req.Payload)
	return &replicationv1.ReplicateLogResponse{Success: err == nil}, err
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	logger := logging.NewDefaultLogger("master_node")
	defer logger.Sync()

	k8sConfig, err := rest.InClusterConfig()
	if err != nil {
		logger.Fatal("Failed to get k8s config (are you running locally?)", zap.Error(err))
	}
	k8sClient := kubernetes.NewForConfigOrDie(k8sConfig)

	state := nodes.NewMasterNodeState()
	_ = state.LoadStateFromFile()
	masterNode := nodes.GetMasterNodeInstance()
	masterNode.IsActive = false

	lis, err := net.Listen("tcp", port)
	if err != nil {
		logger.Fatal("Failed to listen", zap.Error(err))
	}

	grpcServer := grpc.NewServer()
	healthServer := health.NewServer()

	grpc_health_v1.RegisterHealthServer(grpcServer, healthServer)

	healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	log.Println("Health check service registered successfully")

	coordinatorv1.RegisterCoordinatorServiceServer(grpcServer, &server{
		masterNode: masterNode,
		logger:     logger,
	})

	replicationv1.RegisterReplicationServiceServer(grpcServer, &replicationServer{
		masterNode: masterNode,
		logger:     logger,
	})

	go func() {
		logger.Info("gRPC Server listening (waiting for election)", zap.String("address", port))
		if err := grpcServer.Serve(lis); err != nil {
			logger.Fatal("Server failed", zap.Error(err))
		}
	}()
	hostname := os.Getenv("HOSTNAME")
	if hostname == "" {
		hostname = "unknown-node"
	}
	id := fmt.Sprintf("%s-%s", hostname, uuid.New().String())

	lock := &resourcelock.LeaseLock{
		LeaseMeta: metav1.ObjectMeta{
			Name:      "dfs-master-lock",
			Namespace: "datalake",
		},
		Client: k8sClient.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{
			Identity: id,
		},
	}

	logger.Info("Starting Leader Election", zap.String("id", id))

	leaderelection.RunOrDie(context.Background(), leaderelection.LeaderElectionConfig{
		Lock:            lock,
		ReleaseOnCancel: true,
		LeaseDuration:   15 * time.Second,
		RenewDeadline:   10 * time.Second,
		RetryPeriod:     2 * time.Second,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(ctx context.Context) {
				logger.Info(">>> I AM THE MASTER NOW <<<")

				masterNode.IsActive = true

				masterNode.Replicator = nodes.NewReplicator(id)

				if err := masterNode.InitializeLoadBalancer(3, 50051); err != nil {
					logger.Error("Failed to init load balancer", zap.Error(err))
				}

				if err := promoteSelf(k8sClient, hostname, "datalake"); err != nil {
					logger.Error("Failed to patch pod label", zap.Error(err))
				}
			},
			OnStoppedLeading: func() {
				logger.Info(">>> I LOST LEADERSHIP <<<")
				os.Exit(1)
			},
			OnNewLeader: func(identity string) {
				if identity == hostname {
					return
				}
				logger.Info("New leader elected", zap.String("leader", identity))
			},
		},
	})
}

func promoteSelf(client kubernetes.Interface, podName, namespace string) error {
	patchData := []byte(`{"metadata":{"labels":{"role":"active"}}}`)

	_, err := client.CoreV1().Pods(namespace).Patch(
		context.TODO(),
		podName,
		types.StrategicMergePatchType,
		patchData,
		metav1.PatchOptions{},
	)
	return err
}
