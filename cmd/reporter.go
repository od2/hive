package main

import (
	"context"

	"github.com/Shopify/sarama"
	"github.com/spf13/cobra"
	"go.od2.network/hive/pkg/appctx"
	"go.od2.network/hive/pkg/njobs"
	"go.uber.org/zap"
)

var reporterCmd = cobra.Command{
	Use:   "reporter",
	Short: "Run results reporter",
	Args:  cobra.NoArgs,
	Run:   runReporter,
}

func init() {
	rootCmd.AddCommand(&reporterCmd)
}

func runReporter(_ *cobra.Command, _ []string) {
	ctx, cancel := context.WithCancel(appctx.Context())
	defer cancel()
	// Connect to Redis.
	rd := redisClientFromEnv()
	defer func() {
		log.Info("Closing Redis client")
		if err := rd.Close(); err != nil {
			log.Error("Failed to close Redis client", zap.Error(err))
		}
	}()
	topic, partition := kafkaPartitionFromEnv()
	rc := njobs.RedisClient{
		Redis:         rd,
		PartitionKeys: njobs.NewPartitionKeys(topic, partition),
		Scripts:       nil,
		Options:       njobsOptionsFromEnv(),
	}
	// Connect to Kafka.
	saramaClient := saramaClientFromEnv()
	producer, err := sarama.NewSyncProducerFromClient(saramaClient)
	if err != nil {
		log.Fatal("Failed to build Kafka producer", zap.Error(err))
	}
	defer func() {
		log.Info("Closing Kafka producer")
		if err := producer.Close(); err != nil {
			log.Error("Failed to close Kafka consumer", zap.Error(err))
		}
	}()
	// Spin up reporter.
	reporter := njobs.Reporter{
		RedisClient: &rc,
		Producer:    producer,
		Topic:       topic + ".results",
	}
	log.Info("Starting reporter")
	if err := reporter.Run(ctx); err != nil {
		log.Fatal("Reporter failed", zap.Error(err))
	}
}
