package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/razvanmarinn/streaming_ingestion/internal/handlers"
	kf "github.com/razvanmarinn/streaming_ingestion/internal/kafka"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"

	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func initTracer(ctx context.Context) func(context.Context) error {
	conn, err := grpc.DialContext(ctx, "otel-collector.observability.svc.cluster.local:4317", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic(fmt.Sprintf("failed to connect to collector: %v", err))
	}

	exp, err := otlptracegrpc.New(ctx, otlptracegrpc.WithGRPCConn(conn))
	if err != nil {
		panic(fmt.Sprintf("failed to create exporter: %v", err))
	}

	res, _ := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceName("streaming-ingestion"),
		),
	)

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(res),
	)

	otel.SetTracerProvider(tp)
	return tp.Shutdown
}

func main() {
	ctx := context.Background()
	shutdown := initTracer(ctx)
	defer shutdown(ctx)

	kafkaBrokersStr := os.Getenv("KAFKA_BROKERS")
	if kafkaBrokersStr == "" {
		fmt.Println("Warning: KAFKA_BROKERS environment variable not set. Defaulting to localhost:9092.")
		kafkaBrokersStr = "localhost:9092"
	}
	kafkaBrokers := strings.Split(kafkaBrokersStr, ",")
	fmt.Printf("Using Kafka brokers: %v\n", kafkaBrokers)

	kafkaWriter := kf.NewKafkaWriter(kafkaBrokers)

	// ✅ Add otel middleware to your router
	r := handlers.SetupRouter(kafkaWriter)
	r.Use(otelgin.Middleware("streaming-ingestion"))

	r.Run(":8080")
}
