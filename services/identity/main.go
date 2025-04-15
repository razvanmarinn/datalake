package main

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"

	pb "github.com/razvanmarinn/datalake/protobuf"
	"github.com/razvanmarinn/identity_service/internal/db"
	"github.com/razvanmarinn/identity_service/internal/handlers"
	kf "github.com/razvanmarinn/identity_service/internal/kafka"
	"google.golang.org/grpc"
)

func main() {
	database, err := db.Connect_to_db()
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer database.Close()

	s := grpc.NewServer()
	pb.RegisterVerificationServiceServer(s, &handlers.GRPCServer{DB: database})

	var wg sync.WaitGroup
	wg.Add(1)
	lis, err := net.Listen("tcp", ":50056")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	go func() {
		defer wg.Done()
		if err := s.Serve(lis); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
	}()

	kafkaBrokersStr := os.Getenv("KAFKA_BROKERS")
	if kafkaBrokersStr == "" {
		fmt.Println("Warning: KAFKA_BROKERS environment variable not set. Defaulting to localhost:9092.")
		kafkaBrokersStr = "localhost:9092"
	}
	kafkaBrokers := strings.Split(kafkaBrokersStr, ",")
	kafkaWriter := kf.NewKafkaWriter(kafkaBrokers)

	r := handlers.SetupRouter(database, kafkaWriter)
	if err := r.Run(":8082"); err != nil && err != http.ErrServerClosed {
		log.Fatalf("HTTP server failed: %v", err)
	}
}
