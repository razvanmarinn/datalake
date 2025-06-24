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
}

func (s *server) RegisterFile(ctx context.Context, in *pb.ClientFileRequestToMaster) (*pb.MasterFileResponse, error) {
	fm := s.masterNode.RegisterFile(in)

	s.masterNode.FileRegistry = append(s.masterNode.FileRegistry, *fm)
	return &pb.MasterFileResponse{Success: true}, nil
}

func (s *server) GetBatchDestination(ctx context.Context, in *pb.ClientBatchRequestToMaster) (*pb.MasterResponse, error) {
	log.Printf("Received GetBatchDestination request for batch: %v", in.GetBatchId())

	wid, wm := s.masterNode.LoadBalancer.GetNextClient()
	batchuuid, err := uuid.Parse(in.GetBatchId())
	if err != nil {
		fmt.Printf("Asdasda")
	}
	s.masterNode.UpdateBatchLocation(batchuuid, wid)

	return &pb.MasterResponse{WorkerIp: wm.Ip, WorkerPort: wm.Port}, nil
}

func (s *server) GetMetadata(ctx context.Context, in *pb.Location) (*pb.MasterMetadataResponse, error) {
	log.Printf("Received GetMetadata request for file: %v", in.GetFileName())

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
				fmt.Printf("Error getting client for worker ID %s: %v\n", wId.String(), err)
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

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	log.SetOutput(os.Stdout)

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
	})

	log.Printf("gRPC server listening at %v", lis.Addr())

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		if err := s.Serve(lis); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
	}()

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigs
		log.Printf("Received signal: %v", sig)
		log.Println("Shutting down master node and gRPC server...")

		state.UpdateState(masterNode)
		if err := state.SaveState(); err != nil {
			log.Printf("Failed to save master node state: %v", err)
		}

		s.GracefulStop()
		masterNode.Stop()
		log.Println("Master node and gRPC server stopped")
		wg.Done()
	}()

	log.Println("Master node and gRPC server are running. Press Ctrl+C to stop.")
	wg.Wait()
	log.Println("Main function exiting")
}
