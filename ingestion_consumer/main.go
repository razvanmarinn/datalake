package main

import (
	"context"
	"fmt"
	"hash/fnv"
	"log"
	"time"

	pb "github.com/razvanmarinn/datalake/protobuf"

	"github.com/razvanmarinn/ingestion_consumer/internal/batcher"
	"github.com/segmentio/kafka-go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	MaxBatchSize     = 1000             // 64MB
	MinFileSize      = 500              // 10MB
	BatchFlushPeriod = 10 * time.Second // Flush every 10 seconds if not full
)

// func main() {
// 	kafka_topics := []string{"raw_test"}
// 	r := kafka.NewReader(kafka.ReaderConfig{
// 		Brokers:     []string{"localhost:9092"},
// 		Topic:       kafka_topics[0],
// 		Partition:   0,
// 		MinBytes:    10e3, // 10KB
// 		MaxBytes:    10e6, // 10MB
// 		StartOffset: kafka.LastOffset,
// 	})
// 	defer r.Close()

// 	message_batcher := batcher.NewMessageBatch(kafka_topics[0])
// 	ticker := time.NewTicker(BatchFlushPeriod)
// 	defer ticker.Stop()

// 	for {
// 		select {
// 		case <-ticker.C:
// 			if message_batcher.Size() >= MinFileSize {
// 				err := processBatch(message_batcher)
// 				if err != nil {
// 					log.Fatal(err)
// 				}
// 				message_batcher.Clean()
// 			}

// 		default:
// 			m, err := r.ReadMessage(context.Background())
// 			if err != nil {
// 				log.Fatal(err)
// 			}
// 			fmt.Printf("message at topic/partition/offset %v/%v/%v: %s = %s\n", m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))

// 			message_batcher.AddMessage(m.Key, m.Value)

// 			if message_batcher.Size() >= MaxBatchSize {
// 				err := processBatch(message_batcher)
// 				if err != nil {
// 					log.Fatal(err)
// 				}
// 				message_batcher.Clean()
// 			}
// 		}
// 	}
// }

func main() {
	kafka_topics := []string{"raw_test1"}
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{"localhost:9092"},
		Topic:       kafka_topics[0],
		Partition:   0,
		MinBytes:    10e3, // 10KB
		MaxBytes:    10e6, // 10MB
		StartOffset: kafka.LastOffset,
	})
	defer r.Close()

	message_batcher := batcher.NewMessageBatch(kafka_topics[0])

	for i := 0; i < 30; i++ {
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("message at topic/partition/offset %v/%v/%v: %s = %s\n", m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))

		message_batcher.AddMessage(m.Key, m.Value)

	}
	err := processBatch(message_batcher)
	if err != nil {
		log.Fatal(err)
	}
	message_batcher.Clean()

}

func processBatch(msgbatch *batcher.MessageBatch) error {
	err := RegisterFileMetadata(msgbatch)
	if err != nil {
		return err
	}
	fmt.Println("Batch processed successfully")
	return nil
}

func createGRPCConnection(address string) (*grpc.ClientConn, error) {
	return grpc.Dial(address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(64*1024*1024)),
		grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(64*1024*1024)),
	)
}

func combineIpAndPort(ip string, port int32) string {
	if ip[:6] == "worker" {
		ip = "localhost"
	}
	return fmt.Sprintf("%s:%d", ip, port)
}

func createFileRegistrationRequest(msgbatch *batcher.MessageBatch) *pb.ClientFileRequestToMaster {
	t := time.Now()
	fileName := fmt.Sprintf("%s_%s.avro", msgbatch.Topic, t.Format("20060102150405"))

	var totalSize int64
	for _, msg := range msgbatch.Messages {
		totalSize += int64(len(msg.Value))
	}

	req := &pb.ClientFileRequestToMaster{
		FileName:   fileName,
		Hash:       int32(FNV32a(fileName, int(totalSize))),
		FileFormat: "avro",
		FileSize:   totalSize,
		BatchInfo: &pb.Batches{
			Batches: []*pb.Batch{
				{
					Uuid: msgbatch.UUID.String(),
					Size: int32(totalSize),
				},
			},
		},
	}
	return req
}

func registerFileWithMaster(client pb.MasterServiceClient, req *pb.ClientFileRequestToMaster) (*pb.MasterFileResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 600*time.Second)
	defer cancel()
	res, _ := client.RegisterFile(ctx, req)
	return res, nil
}

func getBatchDestination(client pb.MasterServiceClient, batchID string, batchSize int) (*pb.MasterResponse, error) {
	req := &pb.ClientBatchRequestToMaster{
		BatchId:   batchID,
		BatchSize: int32(batchSize),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 600*time.Second)
	defer cancel()

	return client.GetBatchDestination(ctx, req)
}

func sendBatchToWorker(client pb.BatchReceiverServiceClient, batchID string, data []byte) (*pb.WorkerResponse, error) {
	req := &pb.SendClientRequestToWorker{
		BatchId:  batchID,
		Data:     data,
		FileType: "avro",
	}

	ctx, cancel := context.WithTimeout(context.Background(), 600*time.Second)
	defer cancel()

	return client.ReceiveBatch(ctx, req)
}

func RegisterFileMetadata(msgbatch *batcher.MessageBatch) error {
	masterConn, err := createGRPCConnection("localhost:50055")
	if err != nil {
		return fmt.Errorf("failed to connect to master: %v", err)
	}
	defer masterConn.Close()

	masterClient := pb.NewMasterServiceClient(masterConn)

	fileReq := createFileRegistrationRequest(msgbatch)
	fileRes, err := registerFileWithMaster(masterClient, fileReq)
	if err != nil {
		return fmt.Errorf("failed to register file: %v", err)
	}

	destRes, err := getBatchDestination(masterClient, msgbatch.UUID.String(), len(msgbatch.Messages))
	if err != nil {
		return fmt.Errorf("failed to get worker destination: %v", err)
	}

	workerAddr := combineIpAndPort(destRes.GetWorkerIp(), destRes.GetWorkerPort())
	workerConn, err := createGRPCConnection(workerAddr)
	if err != nil {
		return fmt.Errorf("failed to connect to worker: %v", err)
	}
	defer workerConn.Close()

	workerClient := pb.NewBatchReceiverServiceClient(workerConn)
	workerRes, err := sendBatchToWorker(workerClient, msgbatch.UUID.String(), msgbatch.GetMessagesAsBytes())
	if err != nil {
		return fmt.Errorf("failed to send batch to worker: %v", err)
	}

	fmt.Println(workerRes.Success)
	fmt.Printf("Response: %v\n", fileRes)
	return nil
}

func FNV32a(text string, batches int) uint32 {
	algorithm := fnv.New32a()
	algorithm.Write([]byte(text))
	return algorithm.Sum32()
}
