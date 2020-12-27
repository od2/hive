package main

import (
	"time"

	"github.com/BurntSushi/toml"
	"github.com/Shopify/sarama"
	"github.com/go-redis/redis/v8"
	"github.com/spf13/viper"
	"go.od2.network/hive/pkg/njobs"
	"go.uber.org/zap"
)

// Config keys.
const (
	ConfRedisNetwork = "redis.network"
	ConfRedisAddr    = "redis.addr"
	ConfRedisDB      = "redis.db"

	ConfKafkaTopic     = "kafka.topic"
	ConfKafkaPartition = "kafka.partition"

	ConfNJobsTaskAssignments        = "njobs.task_assignments"
	ConfNJobsAssignInterval         = "njobs.assign_interval"
	ConfNJobsAssignBatch            = "njobs.assign_batch"
	ConfNJobsSessionTimeout         = "njobs.session_timeout"
	ConfNJobsSessionRefreshInterval = "njobs.session_refresh_interval"
	ConfNJobsSessionExpireInterval  = "njobs.session_expire_interval"
	ConfNJobsSessionExpireBatch     = "njobs.session_expire_batch"
	ConfNJobsTaskTimeout            = "njobs.task_timeout"
	ConfNJobsTaskExpireInterval     = "njobs.task_expire_interval"
	ConfNJobsTaskExpireBatch        = "njobs.task_expire_batch"
	ConfNJobsDeliverBatch           = "njobs.deliver_batch"

	ConfSaramaAddrs      = "sarama.addrs"
	ConfSaramaConfigFile = "sarama.config_file"
)

func init() {
	viper.SetDefault(ConfRedisNetwork, "tcp")
	viper.SetDefault(ConfRedisAddr, "localhost:6379")
	viper.SetDefault(ConfRedisDB, 0)

	viper.SetDefault(ConfKafkaTopic, "")
	viper.SetDefault(ConfKafkaPartition, int32(0))

	viper.SetDefault(ConfNJobsTaskAssignments, uint(3))
	viper.SetDefault(ConfNJobsAssignInterval, 250*time.Millisecond)
	viper.SetDefault(ConfNJobsAssignBatch, 2048)
	viper.SetDefault(ConfNJobsSessionTimeout, 5*time.Minute)
	viper.SetDefault(ConfNJobsSessionRefreshInterval, 3*time.Second)
	viper.SetDefault(ConfNJobsSessionExpireInterval, 10*time.Second)
	viper.SetDefault(ConfNJobsSessionExpireBatch, uint(16))
	viper.SetDefault(ConfNJobsTaskTimeout, time.Minute)
	viper.SetDefault(ConfNJobsTaskExpireInterval, 2*time.Second)
	viper.SetDefault(ConfNJobsTaskExpireBatch, uint(128))
	viper.SetDefault(ConfNJobsDeliverBatch, uint(2048))

	viper.SetDefault(ConfSaramaAddrs, []string{})
}

func redisClientFromEnv() *redis.Client {
	redisOpts := &redis.Options{
		Network: viper.GetString(ConfRedisNetwork),
		Addr:    viper.GetString(ConfRedisAddr),
		DB:      viper.GetInt(ConfRedisDB),
	}
	log.Info("Connecting to Redis",
		zap.String(ConfRedisNetwork, redisOpts.Network),
		zap.String(ConfRedisAddr, redisOpts.Addr),
		zap.Int(ConfRedisDB, redisOpts.DB))
	return redis.NewClient(redisOpts)
}

func kafkaPartitionFromEnv() (topic string, partition int32) {
	topic = viper.GetString(ConfKafkaTopic)
	partition = viper.GetInt32(ConfKafkaPartition)
	log.Info("Using Kafka parameters",
		zap.String(ConfKafkaTopic, topic),
		zap.Int32(ConfKafkaPartition, partition))
	if topic == "" {
		log.Fatal("Empty " + ConfKafkaTopic)
	}
	return
}

func njobsOptionsFromEnv() *njobs.Options {
	return &njobs.Options{
		TaskAssignments:        viper.GetUint(ConfNJobsTaskAssignments),
		AssignInterval:         viper.GetDuration(ConfNJobsAssignInterval),
		AssignBatch:            viper.GetUint(ConfNJobsAssignBatch),
		SessionTimeout:         viper.GetDuration(ConfNJobsSessionTimeout),
		SessionRefreshInterval: viper.GetDuration(ConfNJobsSessionRefreshInterval),
		SessionExpireInterval:  viper.GetDuration(ConfNJobsSessionExpireInterval),
		SessionExpireBatch:     viper.GetUint(ConfNJobsSessionExpireBatch),
		TaskTimeout:            viper.GetDuration(ConfNJobsTaskTimeout),
		TaskExpireInterval:     viper.GetDuration(ConfNJobsTaskExpireInterval),
		TaskExpireBatch:        viper.GetUint(ConfNJobsTaskExpireBatch),
		DeliverBatch:           viper.GetUint(ConfNJobsDeliverBatch),
	}
}

func saramaClientFromEnv() sarama.Client {
	// Since sarama has so many options, it's easiest to read in a file.
	configFilePath := viper.GetString(ConfSaramaConfigFile)
	if configFilePath == "" {
		log.Fatal("Empty " + ConfSaramaConfigFile)
	}
	log.Info("Reading sarama config",
		zap.String(ConfSaramaConfigFile, configFilePath))
	config := new(sarama.Config)
	if _, err := toml.DecodeFile(configFilePath, config); err != nil {
		log.Fatal("Failed to read sarama config", zap.Error(err))
	}
	// Construct client.
	addrs := viper.GetStringSlice(ConfSaramaAddrs)
	log.Info("Connecting to Kafka (sarama)",
		zap.Strings(ConfSaramaAddrs, addrs))
	client, err := sarama.NewClient(addrs, config)
	if err != nil {
		log.Fatal("Failed to build Kafka (sarama) client", zap.Error(err))
	}
	return client
}
