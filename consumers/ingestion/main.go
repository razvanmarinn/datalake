package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	pb "github.com/razvanmarinn/datalake/protobuf"
	"github.com/razvanmarinn/ingestion_consumer/internal/batcher"
	"github.com/segmentio/kafka-go"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Constants for configuration and behavior
const (
	MaxBatchSize     = 1000
	BatchFlushPeriod = 10 * time.Second
	GRPCMsgSize      = 64 * 1024 * 1024 // 64 MB
	FileFormatAvro   = "avro"
	ServiceName      = "ingestion-consumer"
)

var (
	tracer     = otel.Tracer("ingestion-consumer")
	propagator = propagation.TraceContext{}
)

// Config holds all service configuration.
type Config struct {
	KafkaBrokers       []string
	KafkaTopic         string
	KafkaGroupID       string
	KafkaDLTTopic      string
	SchemaRegistryHost string
	SchemaProjectName  string
	MasterAddress      string
	OtelCollectorAddr  string
}

type IngestMessageBody struct {
	SchemaName string                 `json:"schema_name"`
	ProjectId  string                 `json:"project_id"`
	OwnerId    string                 `json:"owner_id"`
	Data       map[string]interface{} `json:"data"`
}

// App holds the application's dependencies and state.
type App struct {
	config         Config
	kafkaReader    *kafka.Reader
	dltWriter      *kafka.Writer
	tracerProvider *sdktrace.TracerProvider
	logger         *slog.Logger

	// Shared resources with thread-safe access
	batcher       *batcher.Batcher
	batcherLock   sync.Mutex
	schemaCache   *SchemaCache
	grpcConnCache *GRPCConnCache
}

// SchemaCache provides a thread-safe cache for schemas.
type SchemaCache struct {
	mu    sync.RWMutex
	cache map[string]*batcher.Schema
}

func NewSchemaCache() *SchemaCache {
	return &SchemaCache{cache: make(map[string]*batcher.Schema)}
}

func (sc *SchemaCache) Get(key string) (*batcher.Schema, bool) {
	sc.mu.RLock()
	defer sc.mu.RUnlock()
	schema, found := sc.cache[key]
	return schema, found
}

func (sc *SchemaCache) Set(key string, schema *batcher.Schema) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.cache[key] = schema
}

// GRPCConnCache provides a thread-safe cache for gRPC client connections.
type GRPCConnCache struct {
	mu    sync.RWMutex
	conns map[string]*grpc.ClientConn
}

func NewGRPCConnCache() *GRPCConnCache {
	return &GRPCConnCache{conns: make(map[string]*grpc.ClientConn)}
}

func (cc *GRPCConnCache) Get(addr string) (*grpc.ClientConn, error) {
	cc.mu.RLock()
	conn, found := cc.conns[addr]
	cc.mu.RUnlock()

	if found {
		return conn, nil
	}

	cc.mu.Lock()
	defer cc.mu.Unlock()
	// Double-check in case another goroutine created it while we were waiting for the lock.
	conn, found = cc.conns[addr]
	if found {
		return conn, nil
	}

	newConn, err := grpc.Dial(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(GRPCMsgSize)),
		grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(GRPCMsgSize)),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to %s: %w", addr, err)
	}

	cc.conns[addr] = newConn
	return newConn, nil
}

func (cc *GRPCConnCache) CloseAll() {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	for addr, conn := range cc.conns {
		if err := conn.Close(); err != nil {
			slog.Error("failed to close gRPC connection", "address", addr, "error", err)
		}
	}
}

// --- Main Application Logic ---

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	cfg, err := loadConfig()
	if err != nil {
		logger.Error("failed to load configuration", "error", err)
		os.Exit(1)
	}

	app, err := NewApp(cfg, logger)
	if err != nil {
		logger.Error("failed to initialize application", "error", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	shutdownChan := make(chan os.Signal, 1)
	signal.Notify(shutdownChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-shutdownChan
		logger.Info("received shutdown signal", "signal", sig)
		cancel()
		app.Shutdown()
	}()

	app.Run(ctx)
}

func NewApp(cfg Config, logger *slog.Logger) (*App, error) {
	tp, err := initTracer(cfg.OtelCollectorAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize tracer: %w", err)
	}

	tracer = otel.Tracer(ServiceName)

	return &App{
		config:         cfg,
		logger:         logger,
		tracerProvider: tp,
		kafkaReader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:  cfg.KafkaBrokers,
			Topic:    cfg.KafkaTopic,
			GroupID:  cfg.KafkaGroupID,
			MinBytes: 10e3,
			MaxBytes: 10e6,
		}),
		dltWriter: kafka.NewWriter(kafka.WriterConfig{
			Brokers:  cfg.KafkaBrokers,
			Topic:    cfg.KafkaDLTTopic,
			Balancer: &kafka.LeastBytes{},
		}),
		batcher:       batcher.NewBatcher(cfg.KafkaTopic, MaxBatchSize),
		schemaCache:   NewSchemaCache(),
		grpcConnCache: NewGRPCConnCache(),
	}, nil
}

func (app *App) Run(ctx context.Context) {
	app.logger.Info("starting consumer", "topic", app.config.KafkaTopic, "groupID", app.config.KafkaGroupID)
	go app.runTickerFlusher(ctx)

	for {
		select {
		case <-ctx.Done():
			app.logger.Info("context cancelled, stopping message consumption")
			return
		default:
			m, err := app.kafkaReader.ReadMessage(ctx)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					return
				}
				app.logger.Error("error reading message", "error", err)
				continue
			}
			app.processMessage(ctx, m)
		}
	}
}

func (app *App) Shutdown() {
	app.logger.Info("shutting down application...")

	// Flush any remaining messages in the batch
	app.flushBatch(context.Background(), "[Shutdown Flush]")

	// Close Kafka and gRPC connections
	if err := app.kafkaReader.Close(); err != nil {
		app.logger.Error("error closing kafka reader", "error", err)
	}
	if err := app.dltWriter.Close(); err != nil {
		app.logger.Error("error closing DLT writer", "error", err)
	}
	app.grpcConnCache.CloseAll()

	// Shutdown the tracer provider
	if err := app.tracerProvider.Shutdown(context.Background()); err != nil {
		app.logger.Error("error shutting down tracer provider", "error", err)
	}

	app.logger.Info("shutdown complete")
}

func (app *App) runTickerFlusher(ctx context.Context) {
	ticker := time.NewTicker(BatchFlushPeriod)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			app.flushBatch(ctx, "[Flush Timer]")
		case <-ctx.Done():
			app.logger.Info("stopping ticker flusher")
			return
		}
	}
}

func (app *App) processMessage(ctx context.Context, m kafka.Message) {
	// Extract parent context from Kafka headers
	carrier := kafkaHeaderCarrier{headers: &m.Headers}
	ctx = propagator.Extract(ctx, &carrier)

	ctx, span := tracer.Start(ctx, "ProcessKafkaMessage", trace.WithSpanKind(trace.SpanKindConsumer))
	defer span.End()

	logger := app.logger.With("offset", m.Offset, "key", string(m.Key))
	logger.Info("received message %d bytes", "size", len(m.Value))

	var msg IngestMessageBody
	if err := json.Unmarshal(m.Value, &msg); err != nil {
		logger.Error("failed to unmarshal message, sending to DLT", "error", err)
		app.sendToDLT(ctx, m)
		return
	}

	msgSchema, err := app.fetchSchemaWithCache(ctx, string(m.Key))
	if err != nil {
		logger.Error("failed to fetch schema, sending to DLT", "error", err)
		app.sendToDLT(ctx, m)
		return
	}

	unmarshalledData, err := batcher.UnmarshalMessage(msgSchema, m.Value)
	if err != nil {
		logger.Error("failed to unmarshal message, sending to DLT", "error", err)
		app.sendToDLT(ctx, m)
		return
	}

	app.batcherLock.Lock()
	app.batcher.AddMessage(m.Key, m.Value, msg.OwnerId, msg.ProjectId, unmarshalledData)
	size := len(app.batcher.Current.Messages)
	app.batcherLock.Unlock()

	logger.Debug("message added to batch", "current_batch_size", size)

	if size >= MaxBatchSize {
		app.flushBatch(ctx, "[Max Batch Size Reached]")
	}
}

func (app *App) flushBatch(parentCtx context.Context, trigger string) {
	app.batcherLock.Lock()
	if len(app.batcher.Current.Messages) == 0 {
		app.batcherLock.Unlock()
		return
	}

	batchToProcess := app.batcher.Current
	app.batcher.FlushCurrent()
	app.batcherLock.Unlock()

	ctx, span := tracer.Start(parentCtx, "ProcessBatch")
	defer span.End()

	app.logger.Info("triggering batch flush", "trigger", trigger, "message_count", len(batchToProcess.Messages))

	if err := app.registerFileMetadata(ctx, batchToProcess); err != nil {
		app.logger.Error("failed to process batch", "error", err, "batch_uuid", batchToProcess.UUID)
	}
}

func (app *App) registerFileMetadata(ctx context.Context, msgbatch *batcher.MessageBatch) error {
	ctx, span := tracer.Start(ctx, "RegisterFileMetadata")
	defer span.End()

	if len(msgbatch.Messages) == 0 {
		return nil
	}

	// Get Master gRPC client
	masterConn, err := app.grpcConnCache.Get(app.config.MasterAddress)
	if err != nil {
		return err
	}
	masterClient := pb.NewMasterServiceClient(masterConn)

	// Get schema for the first message (assuming all messages in a batch have the same key/schema)
	schema, err := app.fetchSchemaWithCache(ctx, string(msgbatch.Messages[0].Key))
	if err != nil {
		return fmt.Errorf("failed to fetch schema for batch: %w", err)
	}

	avroBytes, err := msgbatch.GetMessagesAsAvroBytes(schema)
	if err != nil {
		return fmt.Errorf("failed to serialize messages to Avro: %w", err)
	}

	// 1. Register File with Master
	fileReq := createFileRegistrationRequest(msgbatch, avroBytes, msgbatch.Messages[0].OwnerId, msgbatch.Messages[0].ProjectId)
	_, err = masterClient.RegisterFile(ctx, fileReq)
	if err != nil {
		return fmt.Errorf("failed to register file with master: %w", err)
	}
	app.logger.Info("file registered with master", "filename", fileReq.FileName, "size", fileReq.FileSize)

	// 2. Get Worker Destination from Master
	destReq := &pb.ClientBatchRequestToMaster{BatchId: msgbatch.UUID.String(), BatchSize: int32(len(msgbatch.Messages))}
	destRes, err := masterClient.GetBatchDestination(ctx, destReq)
	if err != nil {
		return fmt.Errorf("failed to get worker destination: %w", err)
	}
	workerAddr := fmt.Sprintf("%s:%d", destRes.GetWorkerIp(), destRes.GetWorkerPort())
	app.logger.Info("received worker destination", "worker_addr", workerAddr)

	// 3. Send Batch to Worker
	workerConn, err := app.grpcConnCache.Get(workerAddr)
	if err != nil {
		return err
	}
	workerClient := pb.NewBatchReceiverServiceClient(workerConn)
	workerReq := &pb.SendClientRequestToWorker{BatchId: msgbatch.UUID.String(), Data: avroBytes, FileType: FileFormatAvro}
	workerRes, err := workerClient.ReceiveBatch(ctx, workerReq)
	if err != nil {
		return fmt.Errorf("failed to send batch to worker: %w", err)
	}

	app.logger.Info("batch sent to worker successfully", "success", workerRes.Success, "batch_uuid", msgbatch.UUID)
	return nil
}

func (app *App) fetchSchemaWithCache(ctx context.Context, messageKey string) (*batcher.Schema, error) {
	if schema, found := app.schemaCache.Get(messageKey); found {
		return schema, nil
	}

	ctx, span := tracer.Start(ctx, "FetchSchemaFromRegistry")
	defer span.End()

	url := fmt.Sprintf("http://schema-registry.datalake:8081/%s/schema/%s", app.config.SchemaProjectName, messageKey)
	app.logger.Info("fetching schema from registry", "url", url)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create schema request: %w", err)
	}
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch schema: %w", err)
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("schema registry returned status code %d", response.StatusCode)
	}

	var schema batcher.Schema
	if err := json.NewDecoder(response.Body).Decode(&schema); err != nil {
		return nil, fmt.Errorf("failed to decode schema response: %w", err)
	}

	app.schemaCache.Set(messageKey, &schema)
	return &schema, nil
}

func (app *App) sendToDLT(ctx context.Context, m kafka.Message) {
	err := app.dltWriter.WriteMessages(ctx, kafka.Message{
		Key:   m.Key,
		Value: m.Value,
	})
	if err != nil {
		app.logger.Error("failed to write message to DLT", "key", string(m.Key), "error", err)
	}
}

// --- Helper and Utility Functions ---

func loadConfig() (Config, error) {
	kafkaBrokers := os.Getenv("KAFKA_BROKERS")
	if kafkaBrokers == "" {
		return Config{}, errors.New("KAFKA_BROKERS must be set")
	}
	kafkaTopic := os.Getenv("KAFKA_TOPIC")
	if kafkaTopic == "" {
		return Config{}, errors.New("KAFKA_TOPIC must be set")
	}
	groupID := os.Getenv("KAFKA_GROUP_ID")
	if groupID == "" {
		return Config{}, errors.New("KAFKA_GROUP_ID must be set")
	}

	return Config{
		KafkaBrokers:       strings.Split(kafkaBrokers, ","),
		KafkaTopic:         kafkaTopic,
		KafkaGroupID:       groupID,
		KafkaDLTTopic:      kafkaTopic + "-dlt",
		SchemaRegistryHost: getEnv("SCHEMA_REGISTRY_HOST", "schema-registry.datalake:8081"),
		SchemaProjectName:  getEnv("SCHEMA_PROJECT_NAME", "razvan"),
		MasterAddress:      getEnv("MASTER_ADDRESS", "master:50055"),
		OtelCollectorAddr:  getEnv("OTEL_COLLECTOR_ADDR", "otel-collector.observability.svc.cluster.local:4317"),
	}, nil
}

func getEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}

func initTracer(otelAddr string) (*sdktrace.TracerProvider, error) {
	ctx := context.Background()
	conn, err := grpc.DialContext(ctx, otelAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to collector: %w", err)
	}

	exp, err := otlptracegrpc.New(ctx, otlptracegrpc.WithGRPCConn(conn))
	if err != nil {
		return nil, fmt.Errorf("failed to create exporter: %w", err)
	}

	res, _ := resource.New(ctx, resource.WithAttributes(semconv.ServiceName(ServiceName)))

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(res),
	)

	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})

	return tp, nil
}

func createFileRegistrationRequest(msgbatch *batcher.MessageBatch, data []byte, ownerId, projectId string) *pb.ClientFileRequestToMaster {
	totalSize := int64(len(data))
	fileName := fmt.Sprintf("%s_%s.avro", msgbatch.Topic, time.Now().Format("20060102150405"))

	return &pb.ClientFileRequestToMaster{
		FileName:   fileName,
		OwnerId:    ownerId,
		ProjectId:  projectId,
		FileFormat: FileFormatAvro,
		FileSize:   totalSize,
		BatchInfo: &pb.Batches{
			Batches: []*pb.Batch{
				{Uuid: msgbatch.UUID.String(), Size: int32(totalSize)},
			},
		},
	}
}

// func fnv32a(text string) uint32 {
// 	algorithm := fnv.New32a()
// 	algorithm.Write([]byte(text))
// 	return algorithm.Sum32()
// }

// kafkaHeaderCarrier remains the same as in your original code.
type kafkaHeaderCarrier struct {
	headers *[]kafka.Header
}

func (c *kafkaHeaderCarrier) Get(key string) string {
	for _, h := range *c.headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

func (c *kafkaHeaderCarrier) Set(key, val string) {
	for i, h := range *c.headers {
		if h.Key == key {
			(*c.headers)[i].Value = []byte(val)
			return
		}
	}
	*c.headers = append(*c.headers, kafka.Header{
		Key:   key,
		Value: []byte(val),
	})
}

func (c *kafkaHeaderCarrier) Keys() []string {
	keys := make([]string, len(*c.headers))
	for i, h := range *c.headers {
		keys[i] = h.Key
	}
	return keys
}
