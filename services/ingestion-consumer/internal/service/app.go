package service

import (
	"context"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/razvanmarinn/ingestion_consumer/internal/batcher"
	"github.com/razvanmarinn/ingestion_consumer/internal/config"
	"github.com/razvanmarinn/ingestion_consumer/internal/infra"
	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

const (
	ServiceName  = "ingestion-consumer"
	MaxBatchSize = 500
)

type App struct {
	Config config.Config
	Logger *slog.Logger

	// Core Dependencies
	TracerProvider interface{}
	Tracer         trace.Tracer
	HttpClient     *http.Client

	// Kafka Writer (Reader is local to Run loop now)
	DLTWriter     *kafka.Writer
	GrpcConnCache *infra.GRPCConnCache
	// Batching State
	Batcher     *batcher.Batcher
	BatcherLock sync.Mutex // Added this back
	SchemaCache *infra.SchemaCache
}

// NewApp initializes static dependencies. It DOES NOT start the reader.
func NewApp(cfg config.Config, logger *slog.Logger) (*App, error) {
	tp, err := infra.InitTracer(cfg.OtelCollectorAddr, ServiceName)
	if err != nil {
		return nil, err
	}

	return &App{
		Config:         cfg,
		Logger:         logger,
		TracerProvider: tp,
		Tracer:         otel.Tracer(ServiceName),
		HttpClient:     &http.Client{Timeout: 5 * time.Second},
		GrpcConnCache:  infra.NewGRPCConnCache(),
		DLTWriter: kafka.NewWriter(kafka.WriterConfig{
			Brokers:  cfg.KafkaBrokers,
			Topic:    cfg.KafkaDLTTopic,
			Balancer: &kafka.LeastBytes{},
		}),

		Batcher:     batcher.NewBatcher("mixed-topics-batch", MaxBatchSize),
		SchemaCache: infra.NewSchemaCache(),
	}, nil
}

// Run is now the Lifecycle Manager
func (app *App) Run(ctx context.Context) {
	app.Logger.Info("entering application run loop")

	for {
		// Check for shutdown before restarting loop
		if ctx.Err() != nil {
			return
		}

		// --- PHASE 1: DISCOVERY ---
		// Block here until we actually have topics. No errors, just waiting.
		topics, err := app.waitForTopics(ctx)
		if err != nil {
			// Only happens if ctx is cancelled
			return
		}

		// --- PHASE 2: CONSUMPTION ---
		// We have topics! Run the consumer until topics change or error occurs.
		app.Logger.Info("starting consumer group", "topics", topics)
		app.consumeUntilChange(ctx, topics)

		app.Logger.Info("consumer group stopped, restarting lifecycle...")
	}
}

// waitForTopics polls every minute until topics are found
func (app *App) waitForTopics(ctx context.Context) ([]string, error) {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	// Try immediately first
	if topics, err := infra.DiscoverTopics(app.Config.KafkaBrokers, app.Config.KafkaTopicRegex); err == nil && len(topics) > 0 {
		return topics, nil
	} else {
		app.Logger.Info("no topics found matching pattern, waiting...", "pattern", app.Config.KafkaTopicRegex)
	}

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
			topics, err := infra.DiscoverTopics(app.Config.KafkaBrokers, app.Config.KafkaTopicRegex)
			if err != nil {
				app.Logger.Warn("discovery failed, retrying in 1 minute", "error", err)
				continue
			}
			if len(topics) > 0 {
				app.Logger.Info("topics discovered", "count", len(topics))
				return topics, nil
			}
		}
	}
}
