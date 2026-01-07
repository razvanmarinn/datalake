package service

import (
	"context"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	catalogv1 "github.com/razvanmarinn/datalake/protobuf/gen/go/catalog/v1"
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

	TracerProvider interface{}
	Tracer         trace.Tracer
	HttpClient     *http.Client
	DLTProducer    *kafka.Producer

	GrpcConnCache *infra.GRPCConnCache

	Batcher     *batcher.Batcher
	BatcherLock sync.Mutex
	SchemaCache *infra.SchemaCache
}

func NewApp(cfg config.Config, logger *slog.Logger) (*App, error) {
	tp, err := infra.InitTracer(cfg.OtelCollectorAddr, ServiceName)
	if err != nil {
		return nil, err
	}

	dltProducer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": cfg.KafkaBrokers[0],
		"client.id":         ServiceName + "-dlt-producer",
		"acks":              "all",
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

func (app *App) fetchProjectId(ctx context.Context, projectName string) (string, error) {
	ctx, span := app.Tracer.Start(ctx, "fetchProjectId")
	defer span.End()

	catalogConn, err := app.GrpcConnCache.Get(app.Config.SchemaRegistryHost)
	if err != nil {
		return "", err
	}
	catalogClient := catalogv1.NewCatalogServiceClient(catalogConn)

	resp, err := catalogClient.GetProject(ctx, &catalogv1.GetProjectRequest{
		ProjectName: projectName,
	})
	if err != nil {
		return "", err
	}

	return resp.ProjectId, nil
}

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
