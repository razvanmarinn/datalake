package service

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/razvanmarinn/ingestion_consumer/internal/batcher"
	"github.com/razvanmarinn/ingestion_consumer/internal/config"
	"github.com/razvanmarinn/ingestion_consumer/internal/infra"
	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

const (
	MaxBatchSize = 1000
	ServiceName  = "ingestion-consumer"
)

type App struct {
	Config         config.Config
	KafkaReader    *kafka.Reader
	DLTWriter      *kafka.Writer
	TracerProvider *sdktrace.TracerProvider
	Logger         *slog.Logger
	HttpClient     *http.Client
	Tracer         trace.Tracer

	Batcher       *batcher.Batcher
	BatcherLock   sync.Mutex
	SchemaCache   *infra.SchemaCache
	GrpcConnCache *infra.GRPCConnCache
}

func NewApp(cfg config.Config, logger *slog.Logger) (*App, error) {
	tp, err := infra.InitTracer(cfg.OtelCollectorAddr, ServiceName)
	if err != nil {
		return nil, err
	}

	// 1. DISCOVER TOPICS
	// Since kafka-go doesn't support regex subscription directly, we find matching topics first.
	logger.Info("discovering topics", "pattern", cfg.KafkaTopicRegex)
	topics, err := infra.DiscoverTopics(cfg.KafkaBrokers, cfg.KafkaTopicRegex)
	if err != nil {
		return nil, fmt.Errorf("topic discovery failed: %w", err)
	}
	logger.Info("topics discovered", "count", len(topics), "list", topics)

	return &App{
		Config:         cfg,
		Logger:         logger,
		TracerProvider: tp,
		Tracer:         otel.Tracer(ServiceName),
		HttpClient:     &http.Client{Timeout: 5 * time.Second},

		KafkaReader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:     cfg.KafkaBrokers,
			GroupID:     cfg.KafkaGroupID,
			GroupTopics: topics, // Use the discovered list
			MinBytes:    10e3,
			MaxBytes:    10e6,
		}),
		DLTWriter: kafka.NewWriter(kafka.WriterConfig{
			Brokers:  cfg.KafkaBrokers,
			Topic:    cfg.KafkaDLTTopic,
			Balancer: &kafka.LeastBytes{},
		}),

		// Batcher uses the Regex/Pattern name as the "Topic" context for now,
		// or just a generic name, since one batcher handles multiple topics.
		Batcher:       batcher.NewBatcher("mixed-topics-batch", MaxBatchSize),
		SchemaCache:   infra.NewSchemaCache(),
		GrpcConnCache: infra.NewGRPCConnCache(),
	}, nil
}

func (app *App) Shutdown() {
	app.Logger.Info("shutting down application...")
	app.FlushBatch(context.Background(), "[Shutdown Flush]")

	app.KafkaReader.Close()
	app.DLTWriter.Close()
	app.GrpcConnCache.CloseAll()
	app.TracerProvider.Shutdown(context.Background())
	app.Logger.Info("shutdown complete")
}
