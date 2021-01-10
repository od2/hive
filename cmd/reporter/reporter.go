package reporter

import (
	"context"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/jmoiron/sqlx"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.od2.network/hive/cmd/providers"
	"go.od2.network/hive/pkg/items"
	"go.od2.network/hive/pkg/reporter"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

var Cmd = cobra.Command{
	Use:   "reporter",
	Short: "Run result reporting service.",
	Args:  cobra.NoArgs,
	Run: func(cmd *cobra.Command, _ []string) {
		app := providers.NewApp(
			cmd,
			fx.Provide(
				newReporterFlags,
				newReporterItemsStore,
				fx.Annotated{
					Name:   "reporter_consumer",
					Target: newReporterConsumer,
				},
			),
			fx.Invoke(runReporter),
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

// Reporter config keys.
const (
	ConfReporterInterval = "reporter.interval"
	ConfReporterBatch    = "reporter.batch"
)

func init() {
	viper.SetDefault(ConfReporterInterval, 2*time.Second)
	viper.SetDefault(ConfReporterBatch, uint(256))
}

type reporterFlags struct {
	consumerGroup string
	collection    string
	pkType        string
}

func newReporterFlags(cmd *cobra.Command, log *zap.Logger) *reporterFlags {
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
	if consumerGroupName == "" {
		log.Fatal("Empty --consumer-group")
	}
	pkType, err := flags.GetString("pk-type")
	if err != nil {
		panic(err)
	}
	if pkType == "" {
		log.Fatal("Empty --pk-type")
	}
	return &reporterFlags{
		collection:    collection,
		consumerGroup: consumerGroupName,
		pkType:        pkType,
	}
}

func newReporterItemsStore(
	log *zap.Logger,
	flags *reporterFlags,
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

func newReporterConsumer(
	lc fx.Lifecycle,
	log *zap.Logger,
	flags *reporterFlags,
	saramaClient sarama.Client,
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

type reporterIn struct {
	fx.In

	Lifecycle        fx.Lifecycle
	Shutdown         fx.Shutdowner
	Flags            *reporterFlags
	Items            *items.Store
	ReporterConsumer sarama.ConsumerGroup `name:"reporter_consumer"`
}

func runReporter(
	log *zap.Logger,
	inputs reporterIn,
) {
	worker := &reporter.Worker{
		MaxDelay:  viper.GetDuration(ConfReporterInterval),
		BatchSize: viper.GetUint(ConfReporterBatch),
		ItemStore: inputs.Items,
		Log:       log,
	}
	innerCtx, cancel := context.WithCancel(context.Background())
	inputs.Lifecycle.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			go func() {
				defer inputs.Shutdown.Shutdown()
				if err := inputs.ReporterConsumer.Consume(
					innerCtx,
					[]string{inputs.Flags.collection + ".results"},
					worker,
				); err != nil {
					log.Error("Consumer group exited", zap.Error(err))
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
