package main

import (
	"context"
	"fmt"
	"hash/fnv"
	"log"
	"time"

	"github.com/razvanmarinn/ingestion_consumer/internal/batcher"
	pb "github.com/razvanmarinn/ingestion_consumer/proto"
	"github.com/segmentio/kafka-go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	// kf "github.com/razvanmarinn/streaming_ingestion/internal/kafka"
)

func main() {
	kafka_topics := []string{"raw_test"}
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{"localhost:9092"},
		Topic:     kafka_topics[0],
		Partition: 0,
		MinBytes:  10e3, // 10KB
		MaxBytes:  10e6, // 10MB
	})
	defer r.Close()

	message_batcher := batcher.NewMessageBatch(kafka_topics[0])
	for true {
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("message at topic/partition/offset %v/%v/%v: %s = %s\n", m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))

		message_batcher.AddMessage(m.Key, m.Value)
		err = RegisterFileMetadata(message_batcher)
		if err != nil {
			log.Fatal(err)
		}
		// if message_batcher.IsFull() {
		// 	err := RegisterFileMetadata(message_batcher)
		// 	if err != nil {
		// 		log.Fatal(err)
		// 	}
		// 	message_batcher.Clean()
		// 	break
		// }

	}

}

func RegisterFileMetadata(msgbatch *batcher.MessageBatch) error {
	t := time.Now()
	fileName := msgbatch.Topic + fmt.Sprintf(t.Format("20060102150405")) + ".json"
	conn, err := grpc.Dial("localhost:50055", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("failed to connect to master: %v", err)
	}
	defer conn.Close()

	client := pb.NewMasterServiceClient(conn)

	req := &pb.ClientFileRequestToMaster{
		FileName: fileName,
		Hash:     int32(FNV32a(fileName, len(msgbatch.Messages))),
		FileSize: 0,
		BatchInfo: &pb.Batches{
			Batches: make([]*pb.Batch, len(msgbatch.Messages)),
		},
	}

	for i, msg := range msgbatch.Messages {
		req.BatchInfo.Batches[i] = &pb.Batch{
			Uuid: msg.UUID.String(),
			Size: int32(len(msg.Value)),
		}
		req.FileSize += int64(len(msg.Value))
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

func FNV32a(text string, batches int) uint32 {
	algorithm := fnv.New32a()
	algorithm.Write([]byte(text))
	return algorithm.Sum32()
}
