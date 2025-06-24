package client

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"time"

	pb "github.com/razvanmarinn/datalake/protobuf"

	"github.com/google/uuid"
	"github.com/razvanmarinn/rcss/pkg/batches"
	"github.com/razvanmarinn/rcss/pkg/hashing"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const maxMsgSize = 100 * 1024 * 1024

type RCSSClient struct {
	UserId        int
	CurrentMaster string
}

func NewRCSSClient() *RCSSClient {
	return &RCSSClient{
		UserId:        rand.Intn(100),
		CurrentMaster: "master:50055",
	}
}

func (rc *RCSSClient) GetMetadata(fileName string) (*pb.MasterMetadataResponse, error) {
	conn, err := grpc.Dial(rc.CurrentMaster, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to master: %v", err)
	}
	defer conn.Close()

	client := pb.NewMasterServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	req := &pb.Location{FileName: fileName}
	resp, err := client.GetMetadata(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("failed to get metadata: %v", err)
	}

	return resp, nil
}

func (rc *RCSSClient) GetWorkerClient(location string) (pb.BatchReceiverServiceClient, error) {
	conn, err := grpc.Dial(location,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxMsgSize)),
		grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(maxMsgSize)),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to worker: %v", err)
	}
	return pb.NewBatchReceiverServiceClient(conn), nil
}

func (rc *RCSSClient) GetFileBackFromWorkers(fileName string) ([]byte, error) {
	metadata, err := rc.GetMetadata(fileName)
	if err != nil {
		return nil, err
	}

	fileData := make([]byte, 0)
	for _, batchID := range metadata.BatchIds {
		location := metadata.BatchLocations[batchID]
		client, err := rc.GetWorkerClient(location.WorkerIds[0]) // to modify in the future ( where replicas come into play)
		if err != nil {
			return nil, fmt.Errorf("failed to get worker client: %v, %s", err, client)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 600*time.Second)
		defer cancel()

		resp, err := client.RetrieveBatchForClient(ctx, &pb.GetClientRequestToWorker{BatchId: batchID})
		if err != nil {
			return nil, fmt.Errorf("failed to get batch data from worker: %v", err)
		}

		fileData = append(fileData, resp.BatchData...)
	}

	return fileData, nil
}

func (rc *RCSSClient) RegisterFileMetadata(fileName string, batches []batches.Batch) error {
	log.Printf("Trying to register file : %s", fileName)
	conn, err := grpc.Dial(rc.CurrentMaster, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("failed to connect to master: %v", err)
	}
	defer conn.Close()

	client := pb.NewMasterServiceClient(conn)

	req := &pb.ClientFileRequestToMaster{
		FileName: fileName,
		Hash:     int32(hashing.FNV32a(fileName, len(batches))),
		FileSize: 0,
		BatchInfo: &pb.Batches{
			Batches: make([]*pb.Batch, len(batches)),
		},
	}

	for i, batch := range batches {
		req.BatchInfo.Batches[i] = &pb.Batch{
			Uuid: batch.UUID.String(),
			Size: int32(len(batch.Data)),
		}
		req.FileSize += int64(len(batch.Data))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 600*time.Second)
	defer cancel()

	res, err := client.RegisterFile(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to get worker destination: %v", err)
	}

	fmt.Printf("Response: %v\n", res)
	return nil
}

func (rc *RCSSClient) GetBatchDest(batch_id uuid.UUID, batch_size int32) (string, int32, error) {
	conn, err := grpc.Dial(rc.CurrentMaster, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return "", 0, fmt.Errorf("failed to connect to master: %v", err)
	}
	defer conn.Close()

	client := pb.NewMasterServiceClient(conn)
	req := &pb.ClientBatchRequestToMaster{
		BatchId:   batch_id.String(),
		BatchSize: batch_size,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 600*time.Second)
	defer cancel()

	res, err := client.GetBatchDestination(ctx, req)
	return res.GetWorkerIp(), res.GetWorkerPort(), nil
}

func (rc *RCSSClient) SendBatchToWorkers(worker_ip string, worker_port int32, batch_id uuid.UUID, batch_content []byte) error {
	combineIpAndPort := func(ip string, port int32) string {
		return fmt.Sprintf("%s:%d", ip, port)

	}

	conn, err := grpc.Dial(combineIpAndPort(worker_ip, worker_port),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxMsgSize)),
		grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(maxMsgSize)),
	)
	if err != nil {
		return fmt.Errorf("failed to connect to worker: %v", err)
	}

	client := pb.NewBatchReceiverServiceClient(conn)
	req := &pb.SendClientRequestToWorker{
		BatchId: batch_id.String(),
		Data:    batch_content,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 600*time.Second)
	defer cancel()
	res, err := client.ReceiveBatch(ctx, req)

	fmt.Println(res.Success)
	return nil
}
