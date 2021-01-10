package assigner

import (
	"context"
	"errors"
	"time"

	"github.com/Shopify/sarama"
	"github.com/go-redis/redis/v8"
	"github.com/spf13/cobra"
	"go.od2.network/hive/cmd/providers"
	"go.od2.network/hive/pkg/njobs"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

var Cmd = cobra.Command{
	Use:   "assigner",
	Short: "Run task assigner.",
	Long: "Runs the background process responsible for assigning tasks to workers.\n" +
		"Only one assigner is required per partition.\n" +
		"Running multiple assigners is allowed during surge upgrades.",
	Args: cobra.NoArgs,
	Run: func(cmd *cobra.Command, _ []string) {
		app := providers.NewApp(
			cmd,
			fx.Provide(
				fx.Annotated{
					Name:   "assigner_consumer",
					Target: newAssignerConsumer,
				},
				fx.Annotated{
					Name:   "assigner_partition_consumer",
					Target: newAssignerPartitionConsumer,
				},
				fx.Annotated{
					Name:   "reporter_producer",
					Target: newReporterProducer,
				},
			),
			fx.Invoke(runAssigner),
		)
		app.Run()
	},
}

func newAssignerConsumer(
	log *zap.Logger,
	saramaClient sarama.Client,
	lc fx.Lifecycle,
) (sarama.Consumer, error) {
	log.Info("Creating Kafka consumer")
	consumer, err := sarama.NewConsumerFromClient(saramaClient)
	if err != nil {
		log.Fatal("Failed to build Kafka consumer", zap.Error(err))
	}
	lc.Append(fx.Hook{
		OnStop: func(ctx context.Context) error {
			log.Info("Closing Kafka consumer")
			return consumer.Close()
		},
	})
	return consumer, nil
}

type assignerConsumerIn struct {
	fx.In

	Lifecycle   fx.Lifecycle
	Partition   *providers.NJobsPartition
	RedisClient *njobs.RedisClient
	Consumer    sarama.Consumer `name:"assigner_consumer"`
	Meter       metric.Meter
}

func newAssignerPartitionConsumer(log *zap.Logger, inputs assignerConsumerIn) (sarama.PartitionConsumer, error) {
	// Start up Kafka consumer.
	log.Info("Starting Kafka partition consumer")
	// Read consumer offset.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	offset, err := inputs.RedisClient.GetOffset(ctx)
	if errors.Is(err, redis.Nil) {
		offset = sarama.OffsetOldest
	} else if err != nil {
		return nil, err
	}
	partitionConsumer, err := inputs.Consumer.ConsumePartition(
		inputs.Partition.Topic, inputs.Partition.Partition, offset)
	if err != nil {
		return nil, err
	}
	if _, err := inputs.Meter.NewInt64UpDownSumObserver("assigner_high_water_mark_offset",
		func(ctx context.Context, res metric.Int64ObserverResult) {
			res.Observe(partitionConsumer.HighWaterMarkOffset())
		}); err != nil {
		return nil, err
	}
	inputs.Lifecycle.Append(fx.Hook{
		OnStop: func(ctx context.Context) error {
			return partitionConsumer.Close()
		},
	})
	return partitionConsumer, nil
}

func newReporterProducer(
	log *zap.Logger,
	saramaClient sarama.Client,
	lc fx.Lifecycle,
) (sarama.SyncProducer, error) {
	producer, err := sarama.NewSyncProducerFromClient(saramaClient)
	if err != nil {
		return nil, err
	}
	lc.Append(fx.Hook{
		OnStop: func(ctx context.Context) error {
			log.Info("Closing Kafka producer")
			return producer.Close()
		},
	})
	return producer, nil
}

type assignerIn struct {
	fx.In

	Lifecycle   fx.Lifecycle
	Shutdown    fx.Shutdowner
	RedisClient *njobs.RedisClient
	Consumer    sarama.PartitionConsumer `name:"assigner_partition_consumer"`
	Meter       metric.Meter
}

func runAssigner(log *zap.Logger, inputs *assignerIn) {
	// Spin up assigner.
	metrics, err := njobs.NewAssignerMetrics(inputs.Meter)
	if err != nil {
		log.Fatal("Failed to create metrics", zap.Error(err))
	}
	assigner := njobs.Assigner{
		RedisClient: inputs.RedisClient,
		Log:         log,
		Metrics:     metrics,
	}
	inputs.Lifecycle.Append(fx.Hook{
		OnStart: func(_ context.Context) error {
			go func() {
				log.Info("Starting assigner")
				if err := assigner.Run(inputs.Consumer.Messages()); err != nil {
					log.Error("Assigner failed", zap.Error(err))
					if err := inputs.Shutdown.Shutdown(); err != nil {
						log.Fatal("Failed to shut down", zap.Error(err))
					}
				}
			}()
			return nil
		},
	})
}

type reporterIn struct {
	fx.In

	Lifecycle   fx.Lifecycle
	Shutdown    fx.Shutdowner
	Partition   *providers.NJobsPartition
	RedisClient *njobs.RedisClient
	Producer    sarama.SyncProducer `name:"reporter_producer"`
}

func runReporter(log *zap.Logger, inputs *reporterIn) {
	// Spin up reporter.
	reporter := njobs.Reporter{
		RedisClient: inputs.RedisClient,
		Producer:    inputs.Producer,
		Topic:       inputs.Partition.Topic + ".results",
		Log:         log,
	}
	inputs.Lifecycle.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			go func() {
				log.Info("Starting reporter")
				if err := reporter.Run(ctx); err != nil {
					log.Error("Reporter failed", zap.Error(err))
				}
				if err := inputs.Shutdown.Shutdown(); err != nil {
					log.Fatal("Failed to shut down", zap.Error(err))
				}
			}()
			return nil
		},
	})
}
