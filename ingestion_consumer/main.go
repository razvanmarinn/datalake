package main

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/http"
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

func fetchSchema(messageKey string) (*batcher.Schema, error) {
	url := fmt.Sprintf("http://localhost:8081/razvan/schema/%s", messageKey)

	response, err := http.Get(url)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch schema: %v", err)
	}
	defer response.Body.Close()

	var schema batcher.Schema
	err = json.NewDecoder(response.Body).Decode(&schema)
	if err != nil {
		return nil, fmt.Errorf("failed to decode schema response: %v", err)
	}

	return &schema, nil
}

func main() {
	kafka_topics := []string{"raw_test2"}
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{"localhost:9092"},
		Topic:       kafka_topics[0],
		Partition:   0,
		MinBytes:    10e3, // 10KB
		MaxBytes:    10e6, // 10MB
		StartOffset: kafka.LastOffset,
	})
	defer r.Close()

	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{"localhost:9092"},
		Topic:    "raw_test2-dlt",
		Balancer: &kafka.LeastBytes{},
	})

	message_batcher := batcher.NewMessageBatch(kafka_topics[0])

	for i := 0; i < 10; i++ {
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("message at topic/partition/offset %v/%v/%v: %s = %s\n", m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))
		msg_schema, err := fetchSchema(string(m.Key))
		if err != nil {
			log.Fatal(err)
		}
		messageData, err := batcher.UnmarshalMessage(msg_schema, m.Value)
		if err != nil {
			w.WriteMessages(context.Background(), kafka.Message{
				Key:   m.Key,
				Value: m.Value,
			})
			continue
		}

		// Print out the unmarshalled message data
		for fieldName, fieldValue := range messageData {
			fmt.Printf("Field: %s, Value: %v\n", fieldName, fieldValue)
		}

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
		return fmt.Errorf("failed to fetch schema: %v", err)
	}
	schema, err := fetchSchema(string(msgbatch.Messages[0].Key)) // Fetch schema based on key
	if err != nil {
		return fmt.Errorf("failed to fetch schema: %v", err)
	}
	avroBytes, err := msgbatch.GetMessagesAsAvroBytes(schema)
	if err != nil {
		return fmt.Errorf("failed to serialize messages to Avro: %v", err)
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
	workerRes, err := sendBatchToWorker(workerClient, msgbatch.UUID.String(), avroBytes)
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
