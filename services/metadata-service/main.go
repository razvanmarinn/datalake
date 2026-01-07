package main

import (
	"net"

	"github.com/razvanmarinn/datalake/pkg/logging"
	"github.com/razvanmarinn/datalake/pkg/metrics"
	identityv1 "github.com/razvanmarinn/datalake/protobuf/gen/go/identity/v1"
	catalogv1 "github.com/razvanmarinn/datalake/protobuf/gen/go/catalog/v1"
	"github.com/razvanmarinn/metadata-service/internal/db"
	"github.com/razvanmarinn/metadata-service/internal/handlers"
	"github.com/razvanmarinn/metadata-service/internal/kafka"
	"github.com/razvanmarinn/metadata-service/internal/scheduler" // Add this import
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	logger := logging.NewDefaultLogger("metadata-service")
	serviceMetrics := metrics.NewServiceMetrics("metadata-service")

	database, err := db.Connect_to_db(logger)
	if err != nil {
		logger.Fatal("Failed to connect to database", zap.Error(err))
	}
	defer database.Close()

	provisioner, err := kafka.NewProvisioner()
	if err != nil {
		logger.Error("K8s Provisioner disabled (check if running inside K8s)", zap.Error(err))
	}

	conn, err := grpc.Dial("identity-service:50056", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		logger.Fatal("Failed to connect to Identity gRPC service", zap.Error(err))
	}
	defer conn.Close()

	idClient := identityv1.NewIdentityServiceClient(conn)

	r := handlers.SetupRouter(database, logger, provisioner, idClient)

	metrics.SetupMetricsEndpoint(r)
	r.Use(serviceMetrics.PrometheusMiddleware())

	grpcServer := grpc.NewServer()
	metadataGRPCServer := &handlers.GRPCServer{
		DB:     database,
		Logger: logger,
	}
	catalogv1.RegisterCatalogServiceServer(grpcServer, metadataGRPCServer)

	grpcPort := ":50051"
	lis, err := net.Listen("tcp", grpcPort)
	if err != nil {
		logger.Fatal("Failed to listen for gRPC", zap.Error(err))
	}

	go func() {
		logger.Info("Starting gRPC Metadata Service", zap.String("port", grpcPort))
		if err := grpcServer.Serve(lis); err != nil {
			logger.Fatal("gRPC server failed", zap.Error(err))
		}
	}()

	compactionScheduler := scheduler.NewCompactionScheduler(database, logger)
	compactionScheduler.Start()

	logger.Info("Starting Metadata Service on :8081")
	if err := r.Run(":8081"); err != nil {
		logger.Fatal("Router failed", zap.Error(err))
	}
}
