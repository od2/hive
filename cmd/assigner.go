package main

import (
	"context"
	"errors"

	"github.com/Shopify/sarama"
	"github.com/go-redis/redis/v8"
	"github.com/spf13/cobra"
	"go.od2.network/hive/pkg/njobs"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

var assignerCmd = cobra.Command{
	Use:   "assigner",
	Short: "Run task assigner.",
	Long: "Runs the background process responsible for assigning tasks to workers.\n" +
		"Only one assigner is required per partition.\n" +
		"Running multiple assigners is allowed during surge upgrades.",
	Args: cobra.NoArgs,
	Run: func(_ *cobra.Command, _ []string) {
		app := fx.New(
			fx.Provide(providers),
			fx.Invoke(runAssigner),
			fx.Logger(zap.NewStdLog(log)),
		)
		app.Run()
	},
}

func init() {
	rootCmd.AddCommand(&assignerCmd)
}

func runAssigner(
	rc *njobs.RedisClient,
	saramaConfig *sarama.Config,
) {
	// Create metrics.
	meter := otel.GetMeterProvider().Meter("assigner")
	// Connect to Kafka.
	saramaClient := saramaClientFromEnv(saramaConfig)
	defer func() {
		if err := saramaClient.Close(); err != nil {
			log.Error("Failed to close sarama client", zap.Error(err))
		}
	}()
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
	go func() {
		<-ctx.Done()
		log.Info("Context done", zap.Error(ctx.Err()))
		log.Info("Closing Kafka partition consumer")
		if err := partitionConsumer.Close(); err != nil {
			log.Error("Failed to close Kafka partition consumer")
		}
	}()
	if _, err := meter.NewInt64UpDownSumObserver("assigner_high_water_mark_offset",
		func(ctx context.Context, res metric.Int64ObserverResult) {
			res.Observe(partitionConsumer.HighWaterMarkOffset())
		}); err != nil {
		log.Fatal("Failed to create metric observer", zap.Error(err))
	}
	// Spin up assigner.
	metrics, err := njobs.NewAssignerMetrics(meter)
	if err != nil {
		log.Fatal("Failed to create metrics", zap.Error(err))
	}
	assigner := njobs.Assigner{
		RedisClient: rc,
		Log:         log,
		Metrics:     metrics,
	}
	log.Info("Starting assigner")
	if err := assigner.Run(partitionConsumer.Messages()); err != nil {
		log.Fatal("Assigner failed", zap.Error(err))
	}
}
