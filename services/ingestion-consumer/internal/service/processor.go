package service

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"sync"
	"time"

	"github.com/razvanmarinn/datalake/pkg/dfs-client"
	catalogv1 "github.com/razvanmarinn/datalake/protobuf/gen/go/catalog/v1"
	"github.com/razvanmarinn/ingestion_consumer/internal/batcher"
	"github.com/razvanmarinn/ingestion_consumer/internal/infra"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.opentelemetry.io/otel/trace"
)

const (
	ChunkSize = 2 * 1024 * 1024
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

	projectName := batch.Messages[0].ProjectName
	projectId, err := app.fetchProjectId(ctx, projectName)
	if err != nil {
		return err
	}
	key := string(batch.Messages[0].Key)

	schema, err := app.fetchSchema(ctx, projectName, key)
	if err != nil {
		return err
	}

	avroBytes, err := batch.GetMessagesAsAvroBytes(schema)
	if err != nil {
		return err
	}
	dataSize := int64(len(avroBytes))

	fileName := fmt.Sprintf("%s/%s/%s_%s.avro", projectName, key, key, time.Now().Format("20060102150405"))
	app.Logger.Info("ingesting to dfs", "file", fileName)

	f, err := app.DFSClient.Create(ctx, fileName,
		dfs.WithProjectID(projectId),
		dfs.WithOwnerID(batch.Messages[0].OwnerId),
		dfs.WithFormat("avro"),
	)
	if err != nil {
		return err
	}

	if _, err := f.Write(avroBytes); err != nil {
		return err
	}

	if err := f.Close(); err != nil {
		return err
	}

	stat, err := f.Stat()
	if err != nil {
		return err
	}

	catalogConn, err := app.GrpcConnCache.Get(app.Config.CatalogServiceAddress)
	if err != nil {
		return err
	}
	catalogClient := catalogv1.NewCatalogServiceClient(catalogConn)

	for _, block := range stat.Blocks {
		_, err = catalogClient.RegisterDataFile(ctx, &catalogv1.RegisterDataFileRequest{
			ProjectId:  projectId,
			SchemaName: key,
			BlockId:    block.BlockId,
			WorkerId:   block.WorkerId,
			FilePath:   fileName,
			FileSize:   dataSize,
			FileFormat: "avro",
		})
		if err != nil {
			return err
		}
	}

	app.Logger.Info("file registered in catalog", "file", fileName)
	return nil
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
					} else {
						app.Logger.Error("commit failed", "error", err)
					}
				}

			case kafka.Error:
				app.Logger.Error("kafka error", "error", e)
				if e.Code() == kafka.ErrAllBrokersDown {
					return
				}
			}
		}
	}
}
