package nodes

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	replicationv1 "github.com/razvanmarinn/datalake/protobuf/gen/go/replication/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Replicator struct {
	peers []string
}

func NewReplicator(myHostname string) *Replicator {
	peers := []string{}
	for i := 0; i < 3; i++ {
		hostname := fmt.Sprintf("master-%d.master-headless.datalake.svc.cluster.local:50055", i)
		if hostname != myHostname && fmt.Sprintf("master-%d", i) != myHostname {
			peers = append(peers, hostname)
		}
	}
	return &Replicator{peers: peers}
}

func (r *Replicator) SendToQuorum(ctx context.Context, op OperationLogEntry) error {
	successCount := 1
	required := 2

	var wg sync.WaitGroup
	var mu sync.Mutex

	payloadBytes, _ := json.Marshal(op.Payload)

	for _, peer := range r.peers {
		wg.Add(1)
		go func(address string) {
			defer wg.Done()

			conn, err := grpc.Dial(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				return
			}
			defer conn.Close()

			client := replicationv1.NewReplicationServiceClient(conn)
			_, err = client.ReplicateLog(ctx, &replicationv1.ReplicateLogRequest{
				Timestamp: op.Timestamp,
				OpType:    int32(op.OpType),
				Payload:   payloadBytes,
			})

			if err == nil {
				mu.Lock()
				successCount++
				mu.Unlock()
			}
		}(peer)
	}

	wg.Wait()

	if successCount < required {
		return fmt.Errorf("failed to reach quorum: %d/%d", successCount, required)
	}
	return nil
}
