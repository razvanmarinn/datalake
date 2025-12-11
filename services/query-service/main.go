package main

import (
	"context"
	"log"
	"os"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/razvanmarinn/query_service/internal/grpc"
	"github.com/razvanmarinn/query_service/internal/handlers"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
	"go.uber.org/zap"
)

func main() {
	logger, err := zap.NewProduction()
	if err != nil {
		log.Fatalf("can't initialize zap logger: %v", err)
	}
	defer logger.Sync()

	ctx := context.Background()

	// Initialize OpenTelemetry
	shutdown, err := initTracerProvider(ctx)
	if err != nil {
		logger.Fatal("failed to initialize tracer provider", zap.Error(err))
	}
	defer shutdown()

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

	masterClient, err := grpc.NewMasterClient(masterAddress)
	if err != nil {
		logger.Fatal("Failed to create master client", zap.Error(err))
	}
	defer masterClient.Close()

	workerClients := make(map[string]*grpc.WorkerClient)
	for _, addr := range strings.Split(workerAddresses, ",") {
		client, err := grpc.NewWorkerClient(addr)
		if err != nil {
			logger.Fatal("Failed to create worker client", zap.String("address", addr), zap.Error(err))
		}
		workerClients[addr] = client
		defer client.Close()
	}

	queryHandler := handlers.NewQueryHandler(logger, masterClient, workerClients, queryCounter)

	r := gin.Default()
	r.Use(otelgin.Middleware("query-service"))

	// A health check endpoint
	r.GET("/health", func(c *gin.Context) {
		c.JSON(200, gin.H{
			"status": "UP",
		})
	})

	// The main query endpoint
	r.GET("/query", queryHandler.GetData)

	logger.Info("Starting server on port 8086")
	r.Run(":8086")
}

func initTracerProvider(ctx context.Context) (func(), error) {
	otelAgentAddr, ok := os.LookupEnv("OTEL_EXPORTER_OTLP_ENDPOINT")
	if !ok {
		otelAgentAddr = "0.0.0.0:4317"
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceNameKey.String("query-service"),
		),
	)
	if err != nil {
		return nil, err
	}

	traceExporter, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithInsecure(),
		otlptracegrpc.WithEndpoint(otelAgentAddr),
	)
	if err != nil {
		return nil, err
	}

	bsp := sdktrace.NewBatchSpanProcessor(traceExporter)
	tracerProvider := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithResource(res),
		sdktrace.WithSpanProcessor(bsp),
	)
	otel.SetTracerProvider(tracerProvider)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))

	return func() {
		if err := tracerProvider.Shutdown(ctx); err != nil {
			log.Printf("failed to shutdown tracer provider: %v", err)
		}
	}, nil
}
