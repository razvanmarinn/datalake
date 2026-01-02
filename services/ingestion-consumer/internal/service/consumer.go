package service

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/razvanmarinn/ingestion_consumer/internal/batcher"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

type IngestMessageBody struct {
	SchemaName string                 `json:"schema_name"`
	ProjectId  string                 `json:"project_id"`
	OwnerId    string                 `json:"owner_id"`
	Data       map[string]interface{} `json:"data"`
}

/*
processMessage now consumes *kafka.Message from confluent-kafka-go
*/
func (app *App) processMessage(ctx context.Context, m kafka.Message) {
	// ---- OpenTelemetry context extraction (Kafka headers) ----
	carrier := kafkaHeaderCarrier(m.Headers)
	ctx = propagation.TraceContext{}.Extract(ctx, carrier)

	ctx, span := app.Tracer.Start(
		ctx,
		"ProcessKafkaMessage",
		trace.WithSpanKind(trace.SpanKindConsumer),
	)
	defer span.End()

	var msg IngestMessageBody
	if err := json.Unmarshal(m.Value, &msg); err != nil {
		app.sendToDLT(m, err)
		return
	}

	schema, err := app.fetchSchema(ctx, string(m.Key))
	if err != nil {
		app.sendToDLT(m, err)
		return
	}

	data, err := batcher.UnmarshalMessage(schema, m.Value)
	if err != nil {
		app.sendToDLT(m, err)
		return
	}

	app.BatcherLock.Lock()
	app.Batcher.AddMessage(
		m.Key,
		m.Value,
		msg.OwnerId,
		msg.ProjectId,
		data,
	)
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

	url := fmt.Sprintf(
		"http://%s/%s/schema/%s",
		app.Config.SchemaRegistryHost,
		app.Config.SchemaProjectName,
		key,
	)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := app.HttpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("schema registry returned %d", resp.StatusCode)
	}

	var schema batcher.Schema
	if err := json.NewDecoder(resp.Body).Decode(&schema); err != nil {
		return nil, err
	}

	app.SchemaCache.Set(key, &schema)
	return &schema, nil
}

func (app *App) sendToDLT(m kafka.Message, reason error) {
	app.Logger.Error("sending message to DLT", "reason", reason)

	if app.DLTProducer == nil {
		app.Logger.Error("DLT producer not configured")
		return
	}

	err := app.DLTProducer.Produce(
		&kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &app.Config.KafkaDLTTopic,
				Partition: kafka.PartitionAny,
			},
			Key:     m.Key,
			Value:   m.Value,
			Headers: m.Headers,
		},
		nil,
	)

	if err != nil {
		app.Logger.Error("failed to produce to DLT", "error", err)
	}
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

func (app *App) Shutdown() {
	app.Logger.Info("shutting down application resources")

	if app.DLTProducer != nil {
		app.DLTProducer.Flush(5000)
		app.DLTProducer.Close()
	}
}

/*
Kafka header carrier for OpenTelemetry propagation
*/
type kafkaHeaderCarrier []kafka.Header

func (c kafkaHeaderCarrier) Get(key string) string {
	for _, h := range c {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

func (c kafkaHeaderCarrier) Set(key string, value string) {
	// no-op (consumer side)
}

func (c kafkaHeaderCarrier) Keys() []string {
	keys := make([]string, 0, len(c))
	for _, h := range c {
		keys = append(keys, h.Key)
	}
	return keys
}
