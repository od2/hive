package main

import (
	"context"

	"github.com/Shopify/sarama"
	"github.com/go-redis/redis/v8"
	"github.com/spf13/cobra"
	"go.od2.network/hive/pkg/njobs"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

var reporterCmd = cobra.Command{
	Use:   "reporter",
	Short: "Run results reporter",
	Args:  cobra.NoArgs,
	Run: func(_ *cobra.Command, _ []string) {
		app := fx.New(
			fx.Provide(providers),
			fx.Invoke(newReporter),
			fx.Logger(zap.NewStdLog(log)),
		)
		app.Run()
	},
}

func init() {
	rootCmd.AddCommand(&reporterCmd)
}

func newReporter(
	lc fx.Lifecycle,
	shutdown fx.Shutdowner,
	rd *redis.Client,
	saramaConfig *sarama.Config,
) {
	// Connect to Redis.
	topic, partition := kafkaPartitionFromEnv()
	rc := njobs.RedisClient{
		Redis:         rd,
		PartitionKeys: njobs.NewPartitionKeys(topic, partition),
		Scripts:       nil,
		Options:       njobsOptionsFromEnv(),
	}
	// Connect to Kafka.
	saramaConfig.Producer.Return.Successes = true
	saramaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
	saramaConfig.Consumer.Offsets.AutoCommit.Enable = false
	saramaClient := saramaClientFromEnv(saramaConfig)
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
		Log:         log,
	}
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			go func() {
				log.Info("Starting reporter")
				if err := reporter.Run(ctx); err != nil {
					log.Error("Reporter failed", zap.Error(err))
				}
				if err := shutdown.Shutdown(); err != nil {
					log.Fatal("Failed to shut down", zap.Error(err))
				}
			}()
			return nil
		},
	})
}
