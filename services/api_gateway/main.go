package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/gin-gonic/gin"
	"github.com/razvanmarinn/api_gateway/internal/reverse_proxy"
	"github.com/razvanmarinn/datalake/pkg/metrics"
	pb "github.com/razvanmarinn/datalake/protobuf"
	middleware "github.com/razvanmarinn/datalake/pkg/jwt/middleware"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"
	"go.opentelemetry.io/otel/trace" // ADD THIS IMPORT
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func initTracer(ctx context.Context) func(context.Context) error {
	conn, err := grpc.DialContext(ctx, "otel-collector.observability.svc.cluster.local:4317", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("failed to create gRPC connection to collector: %v", err)
	}
	exporter, err := otlptracegrpc.New(ctx, otlptracegrpc.WithGRPCConn(conn))
	if err != nil {
		log.Fatalf("failed to create OTLP trace exporter: %v", err)
	}
	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceName("api-gateway"),
		),
	)
	if err != nil {
		log.Fatalf("failed to create resource: %v", err)
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

	log.Println("OpenTelemetry tracer initialized with propagation")
	return tp.Shutdown
}

// ADD THIS: Debug middleware to verify trace creation
func traceDebugMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		// Check if we have a trace after otelgin middleware
		span := trace.SpanFromContext(c.Request.Context()) // FIXED: use trace.SpanFromContext
		if span.SpanContext().IsValid() {
			log.Printf("[API-GATEWAY] ★ Trace created: %s", span.SpanContext().TraceID().String())
		} else {
			log.Printf("[API-GATEWAY] ✗ No trace context found")
		}
		c.Next()
	}
}

func main() {
	ctx := context.Background()
	shutdown := initTracer(ctx)
	defer func() {
		_ = shutdown(ctx)
	}()

	identity_service_cnn, err := grpc.Dial("identity-service:50055", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect to gRPC server: %v", err)
	}
	vs := pb.NewVerificationServiceClient(identity_service_cnn)

	// Initialize metrics
	gatewayMetrics := metrics.NewGatewayMetrics("api-gateway")

	r := gin.Default()

	// Setup metrics endpoint
	metrics.SetupMetricsEndpoint(r)

	// Order matters: OpenTelemetry first to create traces, then metrics
	r.Use(otelgin.Middleware("api-gateway"))
	r.Use(gatewayMetrics.PrometheusMiddleware())
	r.Use(traceDebugMiddleware()) // ADD THIS: Debug middleware
	r.Use(middleware.AuthMiddleware())

	r.Any("/ingest/:project/", reverse_proxy.StreamingIngestionProxy(vs, "http://streaming-ingestion:8080"))
	r.Any("/schema_registry/:project/*path", reverse_proxy.SchemaRegistryProxy("http://schema-registry:8080"))

	srv := &http.Server{
		Addr:    ":8080",
		Handler: r,
	}

	// Graceful shutdown
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("listen: %s\n", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("Shutting down server...")
	if err := srv.Shutdown(ctx); err != nil {
		log.Fatal("Server forced to shutdown:", err)
	}
}
