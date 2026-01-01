package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/razvanmarinn/ingestion_consumer/internal/batcher"
	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

type IngestMessageBody struct {
	SchemaName string                 `json:"schema_name"`
	ProjectId  string                 `json:"project_id"`
	OwnerId    string                 `json:"owner_id"`
	Data       map[string]interface{} `json:"data"`
}

func (app *App) Run(ctx context.Context) {
	app.Logger.Info("starting consumer", "pattern", app.Config.KafkaTopicRegex)
	go app.runTickerFlusher(ctx)

	for {
		m, err := app.KafkaReader.ReadMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, io.EOF) {
				return
			}
			app.Logger.Error("error reading message", "error", err)
			continue
		}
		app.processMessage(ctx, m)
	}
}

func (app *App) processMessage(ctx context.Context, m kafka.Message) {
	// Propagate Context
	_ = propagation.MapCarrier{} // Adapt to map if needed or use custom carrier
	// Note: You need the custom KafkaHeaderCarrier struct here or in a utils file

	ctx, span := app.Tracer.Start(ctx, "ProcessKafkaMessage", trace.WithSpanKind(trace.SpanKindConsumer))
	defer span.End()

	var msg IngestMessageBody
	if err := json.Unmarshal(m.Value, &msg); err != nil {
		app.sendToDLT(ctx, m, err)
		return
	}

	schema, err := app.fetchSchema(ctx, string(m.Key))
	if err != nil {
		app.sendToDLT(ctx, m, err)
		return
	}

	data, err := batcher.UnmarshalMessage(schema, m.Value)
	if err != nil {
		app.sendToDLT(ctx, m, err)
		return
	}

	app.BatcherLock.Lock()
	app.Batcher.AddMessage(m.Key, m.Value, msg.OwnerId, msg.ProjectId, data)
	size := len(app.Batcher.Current.Messages)
	app.BatcherLock.Unlock()

	if size >= MaxBatchSize {
		app.FlushBatch(ctx, "[Max Batch Size Reached]")
	}
}

func (app *App) fetchSchema(ctx context.Context, key string) (*batcher.Schema, error) {
	if s, found := app.SchemaCache.Get(key); found {
		return s, nil
	}

	url := fmt.Sprintf("http://%s/%s/schema/%s", app.Config.SchemaRegistryHost, app.Config.SchemaProjectName, key)
	req, _ := http.NewRequestWithContext(ctx, "GET", url, nil)

	resp, err := app.HttpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("registry status: %d", resp.StatusCode)
	}

	var s batcher.Schema
	if err := json.NewDecoder(resp.Body).Decode(&s); err != nil {
		return nil, err
	}

	app.SchemaCache.Set(key, &s)
	return &s, nil
}

func (app *App) sendToDLT(ctx context.Context, m kafka.Message, reason error) {
	app.Logger.Error("sending to DLT", "reason", reason)
	app.DLTWriter.WriteMessages(ctx, kafka.Message{Key: m.Key, Value: m.Value})
}

func (app *App) runTickerFlusher(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			app.FlushBatch(ctx, "[Flush Timer]")
		case <-ctx.Done():
			return
		}
	}
}
