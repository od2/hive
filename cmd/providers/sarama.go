package providers

import (
	"context"
	"os"

	"github.com/Shopify/sarama"
	"github.com/pelletier/go-toml"
	"github.com/rcrowley/go-metrics"
	"github.com/spf13/viper"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

// Sarama config keys.
const (
	ConfSaramaAddrs      = "sarama.addrs"
	ConfSaramaConfigFile = "sarama.config_file"
)

func init() {
	viper.SetDefault(ConfSaramaAddrs, []string{})
	viper.SetDefault(ConfSaramaConfigFile, "")
}

// NewSaramaConfig loads the sarama config file and integrates it into the application.
func NewSaramaConfig(log *zap.Logger) (*sarama.Config, error) {
	// Since sarama has so many options, it's easiest to read in a file.
	configFilePath := viper.GetString(ConfSaramaConfigFile)
	if configFilePath == "" {
		log.Fatal("Missing var " + ConfSaramaConfigFile)
	}
	log.Info("Reading sarama config",
		zap.String(ConfSaramaConfigFile, configFilePath))
	config := sarama.NewConfig()
	config.MetricRegistry = metrics.NewPrefixedChildRegistry(metrics.DefaultRegistry, "sarama.")
	f, err := os.Open(configFilePath)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	dec := toml.NewDecoder(f)
	if err := dec.Decode(config); err != nil {
		return nil, err
	}
	config.Producer.Return.Successes = true
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Consumer.Offsets.AutoCommit.Enable = false
	return config, nil
}

// NewSaramaClient creates the sarama client form the default config file and bootstrap servers.
func NewSaramaClient(lc fx.Lifecycle, log *zap.Logger, config *sarama.Config) (sarama.Client, error) {
	// Construct client.
	addrs := viper.GetStringSlice(ConfSaramaAddrs)
	log.Info("Connecting to Kafka (sarama)",
		zap.Strings(ConfSaramaAddrs, addrs))
	client, err := sarama.NewClient(addrs, config)
	if err != nil {
		return nil, err
	}
	lc.Append(fx.Hook{
		OnStop: func(ctx context.Context) error {
			return client.Close()
		},
	})
	return client, nil
}

// GetSaramaConsumerGroup creates a sarama consumer group by name,
// and integrates it into the application lifecycle.
func GetSaramaConsumerGroup(
	lc fx.Lifecycle,
	log *zap.Logger,
	cl sarama.Client,
	consumerGroupName string,
) (sarama.ConsumerGroup, error) {
	log.Info("Binding to Kafka consumer group",
		zap.String("kafka.consumer_group", consumerGroupName))
	consumerGroup, err := sarama.NewConsumerGroupFromClient(consumerGroupName, cl)
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

// NewSaramaSyncProducer creates a sarama.SyncProducer,
// and integrates it into the application lifecycle.
func NewSaramaSyncProducer(
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
