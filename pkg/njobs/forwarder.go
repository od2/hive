package njobs

import (
	"context"
	"fmt"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/Shopify/sarama"
	"github.com/go-redis/redis/v8"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"go.od2.network/hive-api"
	"go.od2.network/hive/pkg/topology"
	"go.uber.org/zap"
)

// forwarder reads task results from Redis Streams and publishes them to Kafka.
//
// It is not safe to run concurrently (will cause duplicate produced messages).
type forwarder struct {
	RedisClient *RedisClient
	Producer    sarama.SyncProducer
	Topic       string
	Log         *zap.Logger
	metrics     *assignerShardMetrics
}

// Run moves task results from Redis Streams to Kafka.
func (r *forwarder) run(ctx context.Context) error {
	for {
		if err := r.step(ctx); err != nil {
			return err
		}
	}
}

// step reads a batch of results from Redis Streams and moves them to Kafka.
func (r *forwarder) step(ctx context.Context) error {
	// Read results from Redis.
	streams, err := r.RedisClient.Redis.XRead(ctx, &redis.XReadArgs{
		Streams: []string{r.RedisClient.PartitionKeys.Results, "0-0"},
		Count:   int64(r.RedisClient.ResultBatch),
		Block:   r.RedisClient.ResultInterval,
	}).Result()
	if err == redis.Nil || (err == nil && len(streams) < 1) {
		// No streams, so sleep.
		backoff := time.NewTimer(r.RedisClient.ResultBackoff)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-backoff.C:
			return nil
		}
	} else if err != nil {
		return err
	}
	stream := streams[0]
	r.Log.Debug("Read batch", zap.Int("result_count", len(stream.Messages)))
	// Transform messages to Kafka.
	msgIDs := make([]string, len(stream.Messages))
	msgs := make([]*sarama.ProducerMessage, len(stream.Messages))
	for i, msg := range stream.Messages {
		workerIDStr, ok := msg.Values["worker"].(string)
		if !ok {
			return fmt.Errorf("missing worker in result")
		}
		workerID, err := strconv.ParseInt(workerIDStr, 10, 64)
		if err != nil {
			return fmt.Errorf("invalid worker ID: %s", workerIDStr)
		}
		statusEnumStr, ok := msg.Values["status"].(string)
		if !ok {
			return fmt.Errorf("missing status in result")
		}
		statusEnum, err := strconv.ParseInt(statusEnumStr, 10, 64)
		if err != nil {
			return fmt.Errorf("invalid status: %s", statusEnumStr)
		}
		item, ok := msg.Values["item"].(string)
		if !ok {
			return fmt.Errorf("empty item ID: %s", msg.ID)
		}
		result := hive.AssignmentResult{
			Report: &hive.AssignmentReport{
				Status: hive.TaskStatus(statusEnum),
			},
			WorkerId:   workerID,
			FinishTime: ptypes.TimestampNow(),
			Locator: &hive.ItemLocator{
				Collection: topology.CollectionOfTopic(r.Topic),
				Id:         item,
			},
		}
		value, err := proto.Marshal(&result)
		if err != nil {
			return fmt.Errorf("invalid AssignmentResult protobuf: %w", err)
		}
		msgIDs[i] = msg.ID
		msgs[i] = &sarama.ProducerMessage{
			Topic: r.Topic,
			Value: sarama.ByteEncoder(value),
		}
	}
	// Send messages to Kafka (synchronous).
	if err := r.Producer.SendMessages(msgs); err != nil {
		return fmt.Errorf("failed to send results to Kafka: %w", err)
	}
	// Remove messages from Redis Stream.
	if err := r.RedisClient.Redis.XDel(ctx, r.RedisClient.PartitionKeys.Results, msgIDs...).Err(); err != nil {
		return fmt.Errorf("failed to mark Redis messages as done: %w", err)
	}
	r.Log.Debug("Flushed batch")
	atomic.AddInt64(&r.metrics.forwardBatches, 1)
	atomic.AddInt64(&r.metrics.forwardResults, int64(len(msgs)))
	return nil
}
