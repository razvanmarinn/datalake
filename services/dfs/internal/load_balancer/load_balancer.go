package load_balancer

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	datanodev1 "github.com/razvanmarinn/datalake/protobuf/gen/go/datanode/v1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type WorkerMetadata struct {
	Client     datanodev1.DataNodeServiceClient
	Ip         string
	Port       int32
	BatchCount int
}

func NewWorkerMetadata(client datanodev1.DataNodeServiceClient, ip string, port int32, bc int) *WorkerMetadata {
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
			fmt.Sprintf("%s-%d.worker-headless:%d", workerAddress, i, basePort),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithDefaultCallOptions(
				grpc.MaxCallRecvMsgSize(64*1024*1024),
				grpc.MaxCallSendMsgSize(64*1024*1024),
			),
		)
		if err != nil {
			log.Printf("did not connect to worker %d: %v", i+1, err)
			continue
		}

		client := datanodev1.NewDataNodeServiceClient(conn)

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)

		req := &datanodev1.GetWorkerInfoRequest{}
		resp, err := client.GetWorkerInfo(ctx, req)
		cancel()

		if err != nil {
			log.Printf("failed to get worker info for worker %d: %v", i+1, err)
			continue
		}

		workerID := resp.WorkerId
		address := fmt.Sprintf("%s-%d.worker-headless", workerAddress, i)

		wMetadata := NewWorkerMetadata(client, address, int32(basePort), 0)
		lb.workerInfo[workerID] = *wMetadata
		log.Printf("Successfully connected to worker node %d with UUID %s", i+1, workerID)
	}

	return lb
}

func (lb *LoadBalancer) GetNextClient() (string, WorkerMetadata) {
	lb.mu.Lock()
	defer lb.mu.Unlock()

	if len(lb.workerInfo) == 0 {
		return "", WorkerMetadata{}
	}

	keys := make([]string, 0, len(lb.workerInfo))
	for key := range lb.workerInfo {
		keys = append(keys, key)
	}

	lb.currentIdx = (lb.currentIdx + 1) % len(keys)
	clientKey := keys[lb.currentIdx]
	wMetadata := lb.workerInfo[clientKey]

	return clientKey, wMetadata
}

func (lb *LoadBalancer) Rotate() (string, WorkerMetadata) {
	return lb.GetNextClient()
}

func (lb *LoadBalancer) Close() {
	fmt.Println("LoadBalancer close called")
}

func (lb *LoadBalancer) GetClientByWorkerID(workerID string) (datanodev1.DataNodeServiceClient, WorkerMetadata, string, int32, error) {
	lb.mu.Lock()
	defer lb.mu.Unlock()

	wm, exists := lb.workerInfo[workerID]
	if !exists {
		return nil, WorkerMetadata{}, "", 0, fmt.Errorf("worker %s not found", workerID)
	}
	return wm.Client, wm, wm.Ip, wm.Port, nil
}
