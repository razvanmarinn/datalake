package main

import (
	"github.com/razvanmarinn/datalake/pkg/logging"
	"github.com/razvanmarinn/datalake/pkg/metrics"
	pb "github.com/razvanmarinn/datalake/protobuf"
	"github.com/razvanmarinn/metadata-service/internal/db"
	"github.com/razvanmarinn/metadata-service/internal/handlers"
	"github.com/razvanmarinn/metadata-service/internal/kafka"
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


	idClient := pb.NewIdentityServiceClient(conn)

	r := handlers.SetupRouter(database, logger, provisioner, idClient)

	metrics.SetupMetricsEndpoint(r)
	r.Use(serviceMetrics.PrometheusMiddleware())

	logger.Info("Starting Metadata Service on :8081")
	if err := r.Run(":8081"); err != nil {
		logger.Fatal("Router failed", zap.Error(err))
	}
}
