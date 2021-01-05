package njobs

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/Shopify/sarama"
	"github.com/go-redis/redis/v8"
	"github.com/golang/protobuf/proto"
	"go.od2.network/hive/pkg/types"
	"go.uber.org/zap"
)

// Reporter reads task results from Redis Streams and publishes them to Kafka.
//
// TODO It's not safe to run concurrently (yet).
type Reporter struct {
	RedisClient *RedisClient
	Producer    sarama.SyncProducer
	Topic       string
	Log         *zap.Logger
}

// Run moves task results from Redis Streams to Kafka.
func (r *Reporter) Run(ctx context.Context) error {
	for {
		if err := r.step(ctx); err != nil {
			return err
		}
	}
}

// step reads a batch of results from Redis Streams and moves them to Kafka.
func (r *Reporter) step(ctx context.Context) error {
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
		offsetStr, ok := msg.Values["offset"].(string)
		if !ok {
			return fmt.Errorf("missing offset in result")
		}
		offset, err := strconv.ParseInt(offsetStr, 10, 64)
		if err != nil {
			return fmt.Errorf("invalid offset: %s", offsetStr)
		}
		result := types.AssignmentResult{
			KafkaPointer: &types.KafkaPointer{
				Partition: 0, // TODO
				Offset:    offset,
			},
			Status:   types.TaskStatus(statusEnum),
			WorkerId: workerID,
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
	return nil
}
