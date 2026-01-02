package service

import (
	"context"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/razvanmarinn/ingestion_consumer/internal/batcher"
	"github.com/razvanmarinn/ingestion_consumer/internal/config"
	"github.com/razvanmarinn/ingestion_consumer/internal/infra"
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

	// Tracing
	TracerProvider interface{}
	Tracer         trace.Tracer
	HttpClient     *http.Client

	// Kafka
	DLTProducer *kafka.Producer

	// Infra
	GrpcConnCache *infra.GRPCConnCache

	// Batching
	Batcher     *batcher.Batcher
	BatcherLock sync.Mutex
	SchemaCache *infra.SchemaCache
}

// NewApp initializes static dependencies. It DOES NOT start consumption.
func NewApp(cfg config.Config, logger *slog.Logger) (*App, error) {
	tp, err := infra.InitTracer(cfg.OtelCollectorAddr, ServiceName)
	if err != nil {
		return nil, err
	}

	dltProducer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": cfg.KafkaBrokers[0],
		"client.id":        ServiceName + "-dlt-producer",
		"acks":             "all",
	})
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
		DLTProducer:    dltProducer,
		Batcher:        batcher.NewBatcher("mixed-topics-batch", MaxBatchSize),
		SchemaCache:    infra.NewSchemaCache(),
	}, nil
}

// Run is the lifecycle manager.
func (app *App) Run(ctx context.Context) {
	app.Logger.Info("entering application run loop")

	for {
		if ctx.Err() != nil {
			return
		}

		// ---- PHASE 1: Topic discovery ----
		topics, err := app.waitForTopics(ctx)
		if err != nil {
			return
		}

		// ---- PHASE 2: Consumption ----
		app.Logger.Info("starting consumer group", "topics", topics)
		app.consumeUntilChange(ctx, topics)

		app.Logger.Info("consumer group stopped, restarting lifecycle")
	}
}

// waitForTopics blocks until at least one topic matches the regex.
func (app *App) waitForTopics(ctx context.Context) ([]string, error) {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	// Immediate attempt
	if topics, err := infra.DiscoverTopics(app.Config.KafkaBrokers, app.Config.KafkaTopicRegex); err == nil && len(topics) > 0 {
		return topics, nil
	}

	app.Logger.Info(
		"no topics found matching pattern, waiting",
		"pattern", app.Config.KafkaTopicRegex,
	)

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
			topics, err := infra.DiscoverTopics(app.Config.KafkaBrokers, app.Config.KafkaTopicRegex)
			if err != nil {
				app.Logger.Warn("topic discovery failed, retrying", "error", err)
				continue
			}
			if len(topics) > 0 {
				app.Logger.Info("topics discovered", "count", len(topics))
				return topics, nil
			}
		}
	}
}
