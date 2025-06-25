package main

import (
	"context"
	"fmt"
	"log" // Import the log package
	"os"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/razvanmarinn/streaming_ingestion/internal/handlers"
	kf "github.com/razvanmarinn/streaming_ingestion/internal/kafka"

	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func initTracer(ctx context.Context) func(context.Context) error {
	fmt.Println("started tracer")
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

	// ⬇️ Critical for traceparent to be extracted correctly!
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})

	return tp.Shutdown
}

// ★★★ ADD THIS DEBUGGING FUNCTION ★★★
// logHeadersMiddleware prints all incoming request headers to the console.
func logHeadersMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		log.Println("--- [DEBUG] Incoming Request Headers ---")
		// Loop over header names
		for key, values := range c.Request.Header {
			// Loop over all values for the name
			for _, value := range values {
				// We convert the key to lowercase to make it easy to search for 'traceparent'
				log.Printf("[DEBUG] %s: %s\n", strings.ToLower(key), value)
			}
		}
		log.Println("--- [DEBUG] End of Headers ---")
		c.Next() // Pass control to the next middleware
	}
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

	r := gin.Default()

	// Your existing OpenTelemetry middleware
	r.Use(otelgin.Middleware("streaming-ingestion"))
	r.Use(logHeadersMiddleware())

	handlers.SetupRouter(r, kafkaWriter)

	r.Run(":8080")
}
