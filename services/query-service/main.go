package main

import (
	"context"
	"log"

	"os"
	"strings"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	middleware "github.com/razvanmarinn/datalake/pkg/jwt/middleware"
	i_grpc "github.com/razvanmarinn/query_service/internal/grpc"
	"github.com/razvanmarinn/query_service/internal/handlers"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	logger, err := zap.NewProduction()
	if err != nil {
		log.Fatalf("can't initialize zap logger: %v", err)
	}
	defer logger.Sync()

	ctx := context.Background()

	// Initialize OpenTelemetry
	shutdown := initTracer(ctx, logger)
	if err != nil {
		logger.Fatal("failed to initialize tracer provider", zap.Error(err))
	}
	defer shutdown(ctx)

	// Meter Provider
	meter := otel.Meter("query-service")
	queryCounter, err := meter.Int64Counter("queries.count")
	if err != nil {
		logger.Fatal("failed to create query counter", zap.Error(err))
	}

	masterAddress := os.Getenv("MASTER_SERVICE_ADDRESS")
	if masterAddress == "" {
		logger.Fatal("MASTER_SERVICE_ADDRESS environment variable not set")
	}

	workerAddresses := os.Getenv("WORKER_SERVICE_ADDRESSES")
	if workerAddresses == "" {
		logger.Fatal("WORKER_SERVICE_ADDRESSES environment variable not set")
	}

	masterClient, err := i_grpc.NewMasterClient(masterAddress)
	if err != nil {
		logger.Fatal("Failed to create master client", zap.Error(err))
	}
	defer masterClient.Close()

	workerClients := make(map[string]*i_grpc.WorkerClient)
	for _, addr := range strings.Split(workerAddresses, ",") {
		client, err := i_grpc.NewWorkerClient(addr)
		if err != nil {
			logger.Fatal("Failed to create worker client", zap.String("address", addr), zap.Error(err))
		}
		workerClients[addr] = client
		defer client.Close()
	}

	queryHandler := handlers.NewQueryHandler(logger, masterClient, workerClients, queryCounter)

	r := gin.New()
	r.Use(gin.Recovery())
	r.Use(otelgin.Middleware("query-service"))
	config := cors.Config{
		AllowOrigins:     []string{"http://localhost:3001"},
		AllowMethods:     []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
		AllowHeaders:     []string{"Content-Type", "Authorization"},
		AllowCredentials: true,
	}
	r.Use(cors.New(config))
	// A health check endpoint
	r.GET("/health", func(c *gin.Context) {
		c.JSON(200, gin.H{
			"status": "UP",
		})
	})

	// The main query endpoint
	r.GET("/query", queryHandler.GetData)
	r.GET("/get_file_list/:project", middleware.AuthMiddleware(), queryHandler.GetFileList)

	logger.Info("Starting server on port 8086")
	r.Run(":8086")
}
func initTracer(ctx context.Context, logger *zap.Logger) func(context.Context) error {
	conn, err := grpc.DialContext(ctx, "otel-collector.observability.svc.cluster.local:4317", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		logger.Fatal("failed to create gRPC connection to collector")
	}
	exporter, err := otlptracegrpc.New(ctx, otlptracegrpc.WithGRPCConn(conn))
	if err != nil {
		logger.Fatal("failed to create OTLP trace exporter:")
	}
	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceName("api-gateway"),
		),
	)
	if err != nil {
		logger.Fatal("failed to create resource")
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)

	// ADD THIS: Configure trace propagation
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	logger.Info("OpenTelemetry tracer initialized with propagation")
	return tp.Shutdown
}
