package service

import (
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"sort"
	"syscall"
	"time"

	pb "github.com/razvanmarinn/datalake/protobuf"
	"github.com/razvanmarinn/ingestion_consumer/internal/batcher"
	"github.com/razvanmarinn/ingestion_consumer/internal/infra"
	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel/trace"
)

func (app *App) FlushBatch(parentCtx context.Context, trigger string) {
	app.BatcherLock.Lock()
	if len(app.Batcher.Current.Messages) == 0 {
		app.BatcherLock.Unlock()
		return
	}
	batch := app.Batcher.Flush()
	app.BatcherLock.Unlock()
	if batch == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Link to parent trace
	ctx, span := app.Tracer.Start(ctx, "ProcessBatch", trace.WithLinks(trace.LinkFromContext(parentCtx)))
	defer span.End()

	app.Logger.Info("flushing batch", "trigger", trigger, "size", len(batch.Messages))

	if err := app.dispatchBatch(ctx, batch); err != nil {
		app.Logger.Error("failed to dispatch batch", "error", err)
	}
}

func (app *App) dispatchBatch(ctx context.Context, batch *batcher.MessageBatch) error {
	// 1. Get Schema
	schema, err := app.fetchSchema(ctx, string(batch.Messages[0].Key))
	if err != nil {
		return err
	}

	// 2. Convert to Avro
	avroBytes, err := batch.GetMessagesAsAvroBytes(schema)
	if err != nil {
		return err
	}

	// 3. Register with Master
	masterConn, err := app.GrpcConnCache.Get(app.Config.MasterAddress)
	if err != nil {
		return err
	}
	masterClient := pb.NewMasterServiceClient(masterConn)

	fName := fmt.Sprintf("%s_%s.avro", batch.Topic, time.Now().Format("20060102150405"))
	fileReq := &pb.ClientFileRequestToMaster{
		FileName:   fName,
		OwnerId:    batch.Messages[0].OwnerId,
		ProjectId:  batch.Messages[0].ProjectId,
		FileFormat: "avro",
		FileSize:   int64(len(avroBytes)),
		BatchInfo: &pb.Batches{
			Batches: []*pb.Batch{{Uuid: batch.UUID.String(), Size: int32(len(avroBytes))}},
		},
	}

	if _, err := masterClient.RegisterFile(ctx, fileReq); err != nil {
		return fmt.Errorf("master reg failed: %w", err)
	}

	// 4. Get Worker Destination
	destRes, err := masterClient.GetBatchDestination(ctx, &pb.ClientBatchRequestToMaster{
		BatchId:   batch.UUID.String(),
		BatchSize: int32(len(batch.Messages)),
	})
	if err != nil {
		return fmt.Errorf("master dest failed: %w", err)
	}

	// 5. Send to Worker
	workerAddr := fmt.Sprintf("%s:%d", destRes.GetWorkerIp(), destRes.GetWorkerPort())
	workerConn, err := app.GrpcConnCache.Get(workerAddr)
	if err != nil {
		return err
	}

	workerClient := pb.NewBatchReceiverServiceClient(workerConn)
	_, err = workerClient.ReceiveBatch(ctx, &pb.SendClientRequestToWorker{
		BatchId:  batch.UUID.String(),
		Data:     avroBytes,
		FileType: "avro",
	})

	return err
}

func (app *App) consumeUntilChange(ctx context.Context, currentTopics []string) {
	// 1. Create the Reader
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     app.Config.KafkaBrokers,
		GroupID:     app.Config.KafkaGroupID,
		GroupTopics: currentTopics,
		MinBytes:    10e3,
		MaxBytes:    10e6,
	})
	defer reader.Close()

	// 2. Create context for this generation
	readerCtx, cancelReader := context.WithCancel(ctx)
	defer cancelReader()

	// --- FIX START: Start the Time-Based Flusher ---
	// This uses the readerCtx so it stops automatically when we reload topics
	go app.runTickerFlusher(readerCtx)
	// --- FIX END ---

	// 3. Start Background Monitor
	go func() {
		ticker := time.NewTicker(1 * time.Minute)
		defer ticker.Stop()

		sort.Strings(currentTopics)

		for {
			select {
			case <-readerCtx.Done():
				return
			case <-ticker.C:
				newTopics, err := infra.DiscoverTopics(app.Config.KafkaBrokers, app.Config.KafkaTopicRegex)
				if err != nil || len(newTopics) == 0 {
					continue
				}

				sort.Strings(newTopics)
				if !reflect.DeepEqual(currentTopics, newTopics) {
					app.Logger.Info("topic change detected", "old", currentTopics, "new", newTopics)
					cancelReader() // Stops reader and flusher
					return
				}
			}
		}
	}()

	// 4. Blocking Read Loop
	for {
		m, err := reader.ReadMessage(readerCtx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			if errors.Is(err, context.DeadlineExceeded) {
				continue
			}
			app.Logger.Error("read message error", "error", err)
			time.Sleep(1 * time.Second)

			// If connection is totally broken, exit to trigger full restart
			if errors.Is(err, io.EOF) || errors.Is(err, syscall.ECONNREFUSED) {
				return
			}
			continue
		}
		app.processMessage(readerCtx, m)
	}
}
