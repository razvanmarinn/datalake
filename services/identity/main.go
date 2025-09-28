package main

import (
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"

	"github.com/gin-gonic/gin"
	"github.com/razvanmarinn/datalake/pkg/logging"
	"github.com/razvanmarinn/datalake/pkg/metrics"
	pb "github.com/razvanmarinn/datalake/protobuf"
	"github.com/razvanmarinn/identity_service/internal/db"
	"github.com/razvanmarinn/identity_service/internal/handlers"
	kf "github.com/razvanmarinn/identity_service/internal/kafka"
	"google.golang.org/grpc"
)

func main() {
	// Set Gin to release mode and disable default logging FIRST
	gin.SetMode(gin.ReleaseMode)
	gin.DefaultWriter = io.Discard
	gin.DefaultErrorWriter = io.Discard

	// Initialize logger
	logger := logging.NewDefaultLogger("identity-service")
	logger.Info("New Logger initialized")
	defer logger.Sync()
	database, err := db.Connect_to_db(logger)
	if err != nil {
		logger.LogStartupError("database", err)
		os.Exit(1)
	}
	defer database.Close()

	s := grpc.NewServer()
	pb.RegisterVerificationServiceServer(s, &handlers.GRPCServer{
		DB:     database,
		Logger: logger,
	})

	var wg sync.WaitGroup
	wg.Add(1)
	lis, err := net.Listen("tcp", ":50056")
	if err != nil {
		logger.LogStartupError("grpc_listener", err)
		os.Exit(1)
	}
	go func() {
		defer wg.Done()
		logger.Info("Starting gRPC server on port 50056")
		if err := s.Serve(lis); err != nil {
			logger.LogStartupError("grpc_server", err)
			os.Exit(1)
		}
	}()

	kafkaBrokersStr := os.Getenv("KAFKA_BROKERS")
	if kafkaBrokersStr == "" {
		logger.Warn("KAFKA_BROKERS environment variable not set, using default")
		kafkaBrokersStr = "localhost:9092"
	}
	kafkaBrokers := strings.Split(kafkaBrokersStr, ",")
	kafkaWriter := kf.NewKafkaWriter(kafkaBrokers)

	// Initialize metrics
	serviceMetrics := metrics.NewServiceMetrics("identity-service")

	r := handlers.SetupRouter(database, kafkaWriter, logger)

	// Setup metrics endpoint
	metrics.SetupMetricsEndpoint(r)

	// Add middleware
	r.Use(serviceMetrics.PrometheusMiddleware())
	r.Use(logger.GinMiddleware())

	logger.Info("Starting HTTP server on port 8082")
	if err := r.Run(":8082"); err != nil && err != http.ErrServerClosed {
		logger.LogStartupError("http_server", err)
		os.Exit(1)
	}
}
