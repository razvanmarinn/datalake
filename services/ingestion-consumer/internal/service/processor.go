package service

import (
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"sort"
	"sync"
	"time"

	commonv1 "github.com/razvanmarinn/datalake/protobuf/gen/go/common/v1"
	coordinatorv1 "github.com/razvanmarinn/datalake/protobuf/gen/go/coordinator/v1"
	datanodev1 "github.com/razvanmarinn/datalake/protobuf/gen/go/datanode/v1"
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
	key := string(batch.Messages[0].Key) // schema name

	schema, err := app.fetchSchema(ctx, projectName, key)
	if err != nil {
		return err
	}

	avroBytes, err := batch.GetMessagesAsAvroBytes(schema)
	if err != nil {
		return err
	}
	dataSize := int64(len(avroBytes))

	masterConn, err := app.GrpcConnCache.Get(app.Config.MasterAddress)
	if err != nil {
		return err
	}
	masterClient := coordinatorv1.NewCoordinatorServiceClient(masterConn)

	allocResp, err := masterClient.AllocateBlock(ctx, &coordinatorv1.AllocateBlockRequest{
		ProjectId: projectId,
		SizeBytes: dataSize,
	})
	if err != nil {
		return fmt.Errorf("allocate block failed: %w", err)
	}

	blockId := allocResp.BlockId
	targets := allocResp.TargetDatanodes

	if len(targets) == 0 {
		return fmt.Errorf("no target datanodes allocated for block %s", blockId)
	}

	target := targets[0]
	workerConn, err := app.GrpcConnCache.Get(target.Address)
	if err != nil {
		return fmt.Errorf("failed to connect to datanode %s: %w", target.Address, err)
	}

	workerClient := datanodev1.NewDataNodeServiceClient(workerConn)
	stream, err := workerClient.PushBlock(ctx)
	if err != nil {
		return fmt.Errorf("failed to open stream to datanode: %w", err)
	}

	err = stream.Send(&datanodev1.PushBlockRequest{
		Data: &datanodev1.PushBlockRequest_Metadata{
			Metadata: &datanodev1.BlockMetadata{
				BlockId:   blockId,
				TotalSize: dataSize,
			},
		},
	})
	if err != nil {
		return fmt.Errorf("failed to send block metadata: %w", err)
	}

	for i := 0; i < len(avroBytes); i += ChunkSize {
		end := i + ChunkSize
		if end > len(avroBytes) {
			end = len(avroBytes)
		}

		err = stream.Send(&datanodev1.PushBlockRequest{
			Data: &datanodev1.PushBlockRequest_Chunk{
				Chunk: avroBytes[i:end],
			},
		})
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to send chunk: %w", err)
		}
	}

	pushResp, err := stream.CloseAndRecv()
	if err != nil {
		return fmt.Errorf("failed to close stream/recv response: %w", err)
	}
	if !pushResp.Success {
		return fmt.Errorf("datanode reported failure: %s", pushResp.Message)
	}

	fileName := fmt.Sprintf("%s/%s/%s_%s.avro", projectId, key, key, time.Now().Format("20060102150405"))
	app.Logger.Info("committing file", "file", fileName, "block_id", blockId)

	_, err = masterClient.CommitFile(ctx, &coordinatorv1.CommitFileRequest{
		ProjectId:  projectId,
		OwnerId:    batch.Messages[0].OwnerId,
		FilePath:   fileName,
		FileFormat: "avro",
		Blocks: []*commonv1.BlockInfo{
			{
				BlockId: blockId,
				Size:    dataSize,
			},
		},
	})
	if err != nil {
		return fmt.Errorf("commit file failed: %w", err)
	}

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
					cancel() // Trigger reconnection
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
