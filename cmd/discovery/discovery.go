package discovery

import (
	"context"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/jmoiron/sqlx"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.od2.network/hive/cmd/providers"
	"go.od2.network/hive/pkg/dedup"
	"go.od2.network/hive/pkg/discovery"
	"go.od2.network/hive/pkg/items"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

var Cmd = cobra.Command{
	Use:   "discovery",
	Short: "Run item discovery service.",
	Args:  cobra.NoArgs,
	Run: func(cmd *cobra.Command, _ []string) {
		app := providers.NewApp(
			cmd,
			fx.Provide(
				newDiscoveryFlags,
				newDiscoveryItemsStore,
				fx.Annotated{
					Name:   "discovered_consumer",
					Target: newDiscoveredConsumer,
				},
				fx.Annotated{
					Name:   "tasks_producer",
					Target: newDiscoveredProducer,
				},
			),
			fx.Invoke(runDiscovery),
		)
		app.Run()
	},
}

func init() {
	flags := Cmd.Flags()
	flags.String("consumer-group", "discovery", "Consumer group name")
	flags.String("collection", "", "Item collection to watch")
	flags.String("pk-type", "", "Primary key type of item collection")
}

// Discovery config keys.
const (
	ConfDiscoveryInterval = "discovery.interval"
	ConfDiscoveryBatch    = "discovery.batch"
)

func init() {
	viper.SetDefault(ConfDiscoveryInterval, 2*time.Second)
	viper.SetDefault(ConfDiscoveryBatch, uint(256))
}

type discoveryFlags struct {
	consumerGroup string
	collection    string
	pkType        string
}

func newDiscoveryFlags(cmd *cobra.Command, log *zap.Logger) *discoveryFlags {
	flags := cmd.Flags()
	collection, err := flags.GetString("collection")
	if err != nil {
		panic(err)
	}
	if collection == "" {
		log.Fatal("Empty --collection")
	}
	consumerGroupName, err := flags.GetString("consumer-group")
	if err != nil {
		panic(err)
	}
	pkType, err := flags.GetString("pk-type")
	if err != nil {
		panic(err)
	}
	if pkType == "" {
		log.Fatal("Empty --pk-type")
	}
	return &discoveryFlags{
		collection:    collection,
		consumerGroup: consumerGroupName,
		pkType:        pkType,
	}
}

func newDiscoveryItemsStore(
	log *zap.Logger,
	flags *discoveryFlags,
	db *sqlx.DB,
) *items.Store {
	store := &items.Store{
		DB:        db,
		TableName: strings.Replace(flags.collection, ".", "_", 1) + "_items",
		PKType:    flags.pkType,
	}
	log.Info("Connecting to items store",
		zap.String("items.table", store.TableName),
		zap.String("items.pkType", flags.pkType))
	return store
}

func newDiscoveredConsumer(
	log *zap.Logger,
	flags *discoveryFlags,
	saramaClient sarama.Client,
	lc fx.Lifecycle,
) (sarama.ConsumerGroup, error) {
	log.Info("Binding to Kafka consumer group",
		zap.String("kafka.consumer_group", flags.consumerGroup))
	consumerGroup, err := sarama.NewConsumerGroupFromClient(flags.consumerGroup, saramaClient)
	if err != nil {
		return nil, err
	}
	lc.Append(fx.Hook{
		OnStop: func(ctx context.Context) error {
			log.Info("Closing Kafka consumer group client")
			return consumerGroup.Close()
		},
	})
	return consumerGroup, nil
}

func newDiscoveredProducer(
	lc fx.Lifecycle,
	log *zap.Logger,
	saramaClient sarama.Client,
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

type discoveryIn struct {
	fx.In

	Lifecycle          fx.Lifecycle
	Shutdown           fx.Shutdowner
	Flags              *discoveryFlags
	Items              *items.Store
	TasksProducer      sarama.SyncProducer  `name:"tasks_producer"`
	DiscoveredConsumer sarama.ConsumerGroup `name:"discovered_consumer"`
}

func runDiscovery(
	log *zap.Logger,
	inputs discoveryIn,
) {
	tasksTopic := inputs.Flags.collection
	discoveryTopic := inputs.Flags.collection + ".discovered"

	// Connect to dedup.
	// TODO Support bitmap dedup.
	deduper := &dedup.SQL{Store: inputs.Items}

	// Set up SQL dedup.
	worker := &discovery.Worker{
		Dedup:     deduper,
		MaxDelay:  viper.GetDuration(ConfDiscoveryInterval),
		BatchSize: viper.GetUint(ConfDiscoveryBatch),

		ItemStore: inputs.Items,
		KafkaSink: &discovery.KafkaSink{
			Producer: inputs.TasksProducer,
			Topic:    tasksTopic,
		},
		Log: log,
	}
	log.Info("Producing new tasks",
		zap.String("kafka.topic", tasksTopic))
	log.Info("Consuming discovered items",
		zap.String("kafka.discovered", discoveryTopic))
	innerCtx, cancel := context.WithCancel(context.Background())
	inputs.Lifecycle.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			go func() {
				if err := inputs.DiscoveredConsumer.Consume(
					innerCtx,
					[]string{discoveryTopic},
					worker,
				); err != nil {
					log.Error("Consumer group exited", zap.Error(err))
					if err := inputs.Shutdown.Shutdown(); err != nil {
						log.Fatal("Failed to shut down", zap.Error(err))
					}
				}
			}()
			return nil
		},
		OnStop: func(ctx context.Context) error {
			cancel()
			return nil
		},
	})
}
