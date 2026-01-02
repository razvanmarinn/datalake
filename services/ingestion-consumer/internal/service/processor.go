package service

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"sync"
	"time"

	pb "github.com/razvanmarinn/datalake/protobuf"
	"github.com/razvanmarinn/ingestion_consumer/internal/batcher"
	"github.com/razvanmarinn/ingestion_consumer/internal/infra"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.opentelemetry.io/otel/trace"
)

var consumerMu sync.Mutex

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

	ctx, span := app.Tracer.Start(
		ctx,
		"ProcessBatch",
		trace.WithLinks(trace.LinkFromContext(parentCtx)),
	)
	defer span.End()

	app.Logger.Info("flushing batch", "trigger", trigger, "size", len(batch.Messages))

	if err := app.dispatchBatch(ctx, batch); err != nil {
		app.Logger.Error("failed to dispatch batch", "error", err)
	}
}
func (app *App) dispatchBatch(ctx context.Context, batch *batcher.MessageBatch) error {
	if len(batch.Messages) == 0 {
		return nil
	}

	// FIX: Extract ProjectId from the first message in the batch
	projectId := batch.Messages[0].ProjectId
	key := string(batch.Messages[0].Key)

	// FIX: Pass projectId as the second argument
	schema, err := app.fetchSchema(ctx, projectId, key)
	if err != nil {
		return err
	}

	avroBytes, err := batch.GetMessagesAsAvroBytes(schema)
	if err != nil {
		return err
	}

	masterConn, err := app.GrpcConnCache.Get(app.Config.MasterAddress)
	if err != nil {
		return err
	}
	masterClient := pb.NewMasterServiceClient(masterConn)

	fileName := fmt.Sprintf("%s_%s.avro", batch.Topic, time.Now().Format("20060102150405"))
	_, err = masterClient.RegisterFile(ctx, &pb.ClientFileRequestToMaster{
		FileName:   fileName,
		OwnerId:    batch.Messages[0].OwnerId,
		ProjectId:  batch.Messages[0].ProjectId,
		FileFormat: "avro",
		FileSize:   int64(len(avroBytes)),
		BatchInfo: &pb.Batches{
			Batches: []*pb.Batch{
				{Uuid: batch.UUID.String(), Size: int32(len(avroBytes))},
			},
		},
	})
	if err != nil {
		return fmt.Errorf("master register failed: %w", err)
	}

	dest, err := masterClient.GetBatchDestination(ctx, &pb.ClientBatchRequestToMaster{
		BatchId:   batch.UUID.String(),
		BatchSize: int32(len(batch.Messages)),
	})
	if err != nil {
		return fmt.Errorf("destination lookup failed: %w", err)
	}

	workerAddr := fmt.Sprintf("%s:%d", dest.WorkerIp, dest.WorkerPort)
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
	consumerMu.Lock()
	defer consumerMu.Unlock()

	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":               app.Config.KafkaBrokers[0],
		"group.id":                        app.Config.KafkaGroupID + "-v2",
		"enable.auto.commit":              false,
		"enable.auto.offset.store":        false,
		"auto.offset.reset":               "earliest",
		"session.timeout.ms":              30000,
		"heartbeat.interval.ms":           3000,
		"go.events.channel.enable":        true,
		"go.application.rebalance.enable": true,
	})
	if err != nil {
		panic(err)
	}
	defer consumer.Close()

	sort.Strings(currentTopics)

	if err := consumer.SubscribeTopics(currentTopics, nil); err != nil {
		panic(err)
	}

	readerCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	go app.runTickerFlusher(readerCtx)

	go func() {
		ticker := time.NewTicker(1 * time.Minute)
		defer ticker.Stop()

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
					cancel()
					return
				}
			}
		}
	}()

	for {
		select {
		case <-readerCtx.Done():
			return

		case ev := <-consumer.Events():
			switch e := ev.(type) {

			case kafka.AssignedPartitions:
				app.Logger.Info("partitions assigned", "partitions", e.Partitions)
				consumer.Assign(e.Partitions)

			case kafka.RevokedPartitions:
				app.Logger.Info("partitions revoked")
				consumer.Unassign()

			case *kafka.Message:
				app.processMessage(readerCtx, *e)

				if _, err := consumer.StoreMessage(e); err != nil {
					app.Logger.Error("offset store failed", "error", err)
				}

				_, err := consumer.Commit()
				if err != nil {
					var kerr kafka.Error
					if errors.As(err, &kerr) && kerr.Code() == kafka.ErrNoOffset {
						// ignore: no offset committed yet
					} else {
						app.Logger.Error("commit failed", "error", err)
					}
				}

			case kafka.Error:
				app.Logger.Error("kafka error", "error", e)
				if e.IsFatal() {
					return
				}
			}
		}
	}
}
