package main

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/http"
	"os"
	"strings"
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
	// Get schema registry host and port from environment or use default service name
	schemaRegistryHost := os.Getenv("SCHEMA_REGISTRY_HOST")
	if schemaRegistryHost == "" {
		schemaRegistryHost = "schema-registry" // Use Kubernetes service name
	}

	schemaRegistryPort := os.Getenv("SCHEMA_REGISTRY_PORT")
	if schemaRegistryPort == "" {
		schemaRegistryPort = "8081" // Default port
	}

	projectName := os.Getenv("SCHEMA_PROJECT_NAME")
	if projectName == "" {
		projectName = "razvan" // Default project name
	}

	url := fmt.Sprintf("http://%s:%s/%s/schema/%s",
		schemaRegistryHost,
		schemaRegistryPort,
		projectName,
		messageKey)

	log.Printf("Fetching schema from: %s", url)

	response, err := http.Get(url)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch schema: %v", err)
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("schema registry returned status code %d", response.StatusCode)
	}

	var schema batcher.Schema
	err = json.NewDecoder(response.Body).Decode(&schema)
	if err != nil {
		return nil, fmt.Errorf("failed to decode schema response: %v", err)
	}

	return &schema, nil
}

func main() {
	kafkaBrokers := os.Getenv("KAFKA_BROKERS")
	kafkaTopic := os.Getenv("KAFKA_TOPIC")
	groupID := os.Getenv("KAFKA_GROUP_ID")

	if kafkaBrokers == "" || kafkaTopic == "" || groupID == "" {
		log.Fatal("KAFKA_BROKERS, KAFKA_TOPIC, and KAFKA_GROUP_ID environment variables must be set.")
	}

	brokerList := strings.Split(kafkaBrokers, ",")
	if len(brokerList) == 0 || brokerList[0] == "" {
		log.Fatal("KAFKA_BROKERS environment variable is empty or malformed after splitting.")
	}
	log.Printf("Connecting to Kafka brokers: %v", brokerList)

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokerList,
		Topic:    kafkaTopic,
		GroupID:  groupID,
		MinBytes: 10e3,
		MaxBytes: 10e6,
	})
	defer r.Close()

	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  brokerList,
		Topic:    kafkaTopic + "-dlt",
		Balancer: &kafka.LeastBytes{},
	})

	messageBatcher := batcher.NewMessageBatch(kafkaTopic)

	// --- Periodic flush goroutine ---
	go func() {
		ticker := time.NewTicker(BatchFlushPeriod)
		defer ticker.Stop()
		for range ticker.C {
			if len(messageBatcher.Messages) > 0 {
				log.Printf("[Flush Timer] Triggered flush with %d messages", len(messageBatcher.Messages))
				if err := processBatch(messageBatcher); err != nil {
					log.Printf("Error processing batch: %v", err)
				}
				messageBatcher.Clean()
			}
		}
	}()

	// --- Main Kafka consume loop ---
	for {
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			log.Printf("Error reading message from topic %s: %v", kafkaTopic, err)
			break
		}
		log.Printf("Received message at offset %d, key=%s", m.Offset, string(m.Key))

		msgSchema, err := fetchSchema(string(m.Key))
		if err != nil {
			log.Printf("Error fetching schema for message key %s: %v", string(m.Key), err)
			_ = w.WriteMessages(context.Background(), kafka.Message{Key: m.Key, Value: m.Value})
			continue
		}

		_, err = batcher.UnmarshalMessage(msgSchema, m.Value)
		if err != nil {
			log.Printf("Error unmarshalling message: %v", err)
			_ = w.WriteMessages(context.Background(), kafka.Message{Key: m.Key, Value: m.Value})
			continue
		}

		messageBatcher.AddMessage(m.Key, m.Value)
		log.Printf("Current batch size: %d messages", len(messageBatcher.Messages))

		if len(messageBatcher.Messages) >= MaxBatchSize {
			log.Printf("[Max Batch Size Reached] Triggering flush with %d messages", len(messageBatcher.Messages))
			err := processBatch(messageBatcher)
			if err != nil {
				log.Printf("Error processing batch: %v", err)
			}
			messageBatcher.Clean()
		}
	}
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
	masterConn, err := createGRPCConnection("master:50055")
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
