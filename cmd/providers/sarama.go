package providers

import (
	"context"
	"os"

	"github.com/Shopify/sarama"
	"github.com/pelletier/go-toml"
	"github.com/spf13/viper"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

const (
	ConfSaramaAddrs      = "sarama.addrs"
	ConfSaramaConfigFile = "sarama.config_file"
)

func init() {
	viper.SetDefault(ConfSaramaAddrs, []string{})
	viper.SetDefault(ConfSaramaConfigFile, "")
}

func NewSaramaConfig(log *zap.Logger) (*sarama.Config, error) {
	// Since sarama has so many options, it's easiest to read in a file.
	configFilePath := viper.GetString(ConfSaramaConfigFile)
	if configFilePath == "" {
		log.Fatal("Empty " + ConfSaramaConfigFile)
	}
	log.Info("Reading sarama config",
		zap.String(ConfSaramaConfigFile, configFilePath))
	config := sarama.NewConfig()
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
