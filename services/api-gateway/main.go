package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/razvanmarinn/api_gateway/internal/reverse_proxy"
	middleware "github.com/razvanmarinn/datalake/pkg/jwt/middleware"
	"github.com/razvanmarinn/datalake/pkg/logging"
	"github.com/razvanmarinn/datalake/pkg/metrics"
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

func initTracer(ctx context.Context, logger *logging.Logger) func(context.Context) error {
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

	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	logger.Info("OpenTelemetry tracer initialized with propagation")
	return tp.Shutdown
}

func main() {
	ctx := context.Background()
	logger := logging.NewDefaultLogger("identity-service")
	logger.Info("New Logger initialized")
	defer logger.Sync()
	shutdown := initTracer(ctx, logger)
	defer func() {
		_ = shutdown(ctx)
	}()

	gatewayMetrics := metrics.NewGatewayMetrics("api-gateway")

	r := gin.Default()
	corsConfig := cors.Config{
		AllowOrigins:     []string{"http://localhost:3001"},
		AllowMethods:     []string{"GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"},
		AllowHeaders:     []string{"Origin", "Content-Length", "Content-Type", "Authorization"},
		AllowCredentials: true,
		MaxAge:           12 * time.Hour,
	}
	r.Use(cors.New(corsConfig))
	metrics.SetupMetricsEndpoint(r)

	r.Use(otelgin.Middleware("api-gateway"))
	r.Use(gatewayMetrics.PrometheusMiddleware())

	public := r.Group("/auth")

	public.Any("/*path", reverse_proxy.GenericProxy("http://identity-service:8082", logger, "/auth"))

	auth := r.Group("/")
	auth.Use(middleware.AuthMiddleware())
	{
		auth.Any("/ingest/:project", reverse_proxy.StreamingIngestionProxy("http://streaming-ingestion:8080", logger))
		auth.Any("/schema_registry/:project/*path", reverse_proxy.MetadataServiceProxy("http://metadata-service:8080", logger))
		auth.Any("/query/*path", reverse_proxy.QueryServiceProxy("http://query-service:8086", logger))
		auth.Any("/meta/*path", reverse_proxy.GenericProxy("http://metadata-service:8081", logger, "/meta"))
	}

	srv := &http.Server{
		Addr:    ":8080",
		Handler: r,
	}

	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Fatal("listen: %s\n")
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	logger.Info("Shutting down server...")
	if err := srv.Shutdown(ctx); err != nil {
		logger.Fatal("Server forced to shutdown")
	}
}
