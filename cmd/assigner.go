package main

import (
	"context"
	"errors"

	"github.com/Shopify/sarama"
	"github.com/go-redis/redis/v8"
	"github.com/spf13/cobra"
	"go.od2.network/hive/pkg/njobs"
	"go.uber.org/zap"
)

var assignerCmd = cobra.Command{
	Use:   "assigner",
	Short: "Run task assigner.",
	Long: "Runs the background process responsible for assigning tasks to workers.\n" +
		"Only one assigner is required per partition.\n" +
		"Running multiple assigners is allowed during surge upgrades.",
	Args: cobra.NoArgs,
	Run:  runAssigner,
}

func init() {
	rootCmd.AddCommand(&assignerCmd)
}

func runAssigner(cmd *cobra.Command, _ []string) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Connect to Redis.
	rd := redisClientFromEnv()
	scripts, err := njobs.LoadScripts(ctx, rd)
	if err != nil {
		log.Fatal("Failed to load njobs scripts", zap.Error(err))
	}
	defer func() {
		log.Info("Closing Redis client")
		if err := rd.Close(); err != nil {
			log.Error("Failed to close Redis client", zap.Error(err))
		}
	}()
	// Connect to njobs system.
	topic, partition := kafkaPartitionFromEnv()
	rc := njobs.RedisClient{
		Redis:         rd,
		PartitionKeys: njobs.NewPartitionKeys(topic, partition),
		Scripts:       scripts,
		Options:       njobsOptionsFromEnv(),
	}
	// Connect to Kafka.
	saramaClient := saramaClientFromEnv()
	log.Info("Creating Kafka consumer")
	consumer, err := sarama.NewConsumerFromClient(saramaClient)
	if err != nil {
		log.Fatal("Failed to build Kafka consumer", zap.Error(err))
	}
	defer func() {
		log.Info("Closing Kafka consumer")
		if err := consumer.Close(); err != nil {
			log.Error("Failed to close Kafka consumer", zap.Error(err))
		}
	}()
	// Read consumer offset.
	offset, err := rc.GetOffset(ctx)
	if errors.Is(err, redis.Nil) {
		offset = sarama.OffsetOldest
	} else if err != nil {
		log.Fatal("Failed to read Kafka consumer offset", zap.Error(err))
	}
	// Start up Kafka consumer.
	log.Info("Starting Kafka partition consumer")
	partitionConsumer, err := consumer.ConsumePartition(topic, partition, offset)
	if err != nil {
		log.Fatal("Failed to start Kafka partition consumer", zap.Error(err))
	}
	defer func() {
		log.Info("Closing Kafka partition consumer")
		if err := partitionConsumer.Close(); err != nil {
			log.Error("Failed to close Kafka partition consumer")
		}
	}()
	// Spin up assigner.
	assigner := njobs.Assigner{
		RedisClient: &rc,
		Options:     njobsOptionsFromEnv(),
	}
	log.Info("Starting assigner")
	if err := assigner.Run(partitionConsumer.Messages()); err != nil {
		log.Fatal("Assigner failed", zap.Error(err))
	}
}
