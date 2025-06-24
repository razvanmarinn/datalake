package load_balancer

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	pb "github.com/razvanmarinn/datalake/protobuf"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type WorkerMetadata struct {
	Client     pb.LBServiceClient
	Ip         string
	Port       int32
	BatchCount int
}

func NewWorkerMetadata(client pb.LBServiceClient, ip string, port int32, bc int) *WorkerMetadata {
	return &WorkerMetadata{
		Client:     client,
		Ip:         ip,
		Port:       port,
		BatchCount: bc,
	}
}

type LoadBalancer struct {
	workerInfo map[string]WorkerMetadata
	currentIdx int
	mu         sync.Mutex
}

const workerAddress = "worker"
const MAXIMUM_BATCHES_PER_WORKER = 100

func NewLoadBalancer(numWorkers int, basePort int) *LoadBalancer {
	lb := &LoadBalancer{
		workerInfo: make(map[string]WorkerMetadata),
	}

	for i := 0; i < numWorkers; i++ {
		conn, err := grpc.Dial(
			fmt.Sprintf("%s-%d:%d", workerAddress, i+1, basePort+i),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithDefaultCallOptions(
				grpc.MaxCallRecvMsgSize(64*1024*1024),
				grpc.MaxCallSendMsgSize(64*1024*1024),
			),
		)
		if err != nil {
			log.Printf("did not connect to worker %d: %v", i+1, err)
		}

		client := pb.NewLBServiceClient(conn)

		// Make an initial request to get the worker ID
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		req := &pb.WorkerIDRequest{} // No parameters needed
		resp, err := client.GetWorkerID(ctx, req)
		if err != nil {
			log.Printf("failed to get worker ID for worker %d: %v", i+1, err)
		}

		workerID := resp.WorkerId.Value
		wMetadata := NewWorkerMetadata(client, fmt.Sprintf("%s-%d", workerAddress, i+1), int32(basePort+i), 0)
		lb.workerInfo[workerID] = *wMetadata
		log.Printf("Successfully connected to worker node %d with UUID %s", i+1, workerID)
	}

	return lb
}

func (lb *LoadBalancer) GetNextClient() (string, WorkerMetadata) {
	wId, wm := lb.Rotate()
	return wId, wm
}

func (lb *LoadBalancer) Rotate() (string, WorkerMetadata) {
	lb.mu.Lock()
	defer lb.mu.Unlock()

	keys := make([]string, 0, len(lb.workerInfo))
	for key := range lb.workerInfo {
		keys = append(keys, key)
	}

	lb.currentIdx = (lb.currentIdx + 1) % len(keys)

	clientKey := keys[lb.currentIdx]
	wMetadata := lb.workerInfo[clientKey]

	return clientKey, wMetadata
}

func (lb *LoadBalancer) Close() {
	for _, wm := range lb.workerInfo {
		if conn, ok := wm.Client.(grpc.ClientConnInterface); ok {
			// conn.Close()
			fmt.Println("Connection closed", conn)
		}
	}
}

func (lb *LoadBalancer) GetClientByWorkerID(workerID string) (pb.LBServiceClient, string, int32, error) {
	lb.mu.Lock()
	defer lb.mu.Unlock()

	wm := lb.workerInfo[workerID]
	return wm.Client, wm.Ip, wm.Port, nil
}
