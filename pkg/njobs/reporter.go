package njobs

import (
	"context"
	"fmt"
	"time"

	"github.com/Shopify/sarama"
	"github.com/go-redis/redis/v8"
	"github.com/golang/protobuf/proto"
	"go.od2.network/hive/pkg/types"
)

// Reporter reads task results from Redis Streams and publishes them to Kafka.
//
// TODO It's not safe to run concurrently (yet).
type Reporter struct {
	RedisClient *RedisClient
	Producer    sarama.SyncProducer
	Topic       string
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
	// Transform messages to Kafka.
	msgIDs := make([]string, len(stream.Messages))
	msgs := make([]*sarama.ProducerMessage, len(stream.Messages))
	for i, msg := range stream.Messages {
		workerID, ok := msg.Values["worker"].(int64)
		if !ok {
			return fmt.Errorf("missing worker in result")
		}
		statusEnum, ok := msg.Values["status"].(int64)
		if !ok {
			return fmt.Errorf("missing status in result")
		}
		offset, ok := msg.Values["offset"].(int64)
		if !ok {
			return fmt.Errorf("missing offset in result")
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
	return r.RedisClient.Redis.XDel(ctx, r.RedisClient.PartitionKeys.Results, msgIDs...).Err()
}
