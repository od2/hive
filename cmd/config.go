package main

import (
	"fmt"
	"net"
	"os"
	"time"

	"github.com/Shopify/sarama"
	"github.com/go-redis/redis/v8"
	"github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/pelletier/go-toml"
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
	ConfNJobsResultInterval         = "njobs.result_interval"
	ConfNJobsResultBatch            = "njobs.result_batch"
	ConfNJobsResultBackoff          = "njobs.result_backoff"

	ConfDiscoveryInterval = "discovery.interval"
	ConfDiscoveryBatch    = "discovery.batch"

	ConfSaramaAddrs      = "sarama.addrs"
	ConfSaramaConfigFile = "sarama.config_file"

	ConfMySQLDSN = "mysql.dsn"

	ConfAuthgwSecret         = "authgw.secret"
	ConfAuthgwCacheSize      = "authgw.cache.size"
	ConfAuthgwCacheTTL       = "authgw.cache.ttl"
	ConfAuthgwCacheStreamKey = "authgw.cache.stream_key"
	ConfAuthgwCacheBacklog   = "authgw.cache.backlog"
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
	viper.SetDefault(ConfNJobsResultInterval, 3*time.Second)
	viper.SetDefault(ConfNJobsResultBatch, uint(64))
	viper.SetDefault(ConfNJobsResultBackoff, 2*time.Second)

	viper.SetDefault(ConfDiscoveryInterval, 2*time.Second)
	viper.SetDefault(ConfDiscoveryBatch, uint(256))

	viper.SetDefault(ConfSaramaAddrs, []string{})
	viper.SetDefault(ConfSaramaConfigFile, "")

	viper.SetDefault(ConfMySQLDSN, "")

	viper.SetDefault(ConfAuthgwSecret, "")
	viper.SetDefault(ConfAuthgwCacheSize, 1024)
	viper.SetDefault(ConfAuthgwCacheTTL, time.Hour)
	viper.SetDefault(ConfAuthgwCacheStreamKey, "token-invalidations")
	viper.SetDefault(ConfAuthgwCacheBacklog, 64)
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
		ResultInterval:         viper.GetDuration(ConfNJobsResultInterval),
		ResultBatch:            viper.GetUint(ConfNJobsResultBatch),
		ResultBackoff:          viper.GetDuration(ConfNJobsResultBackoff),
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
	config := sarama.NewConfig()
	f, err := os.Open(configFilePath)
	if err != nil {
		log.Fatal("Failed to open sarama config", zap.Error(err))
	}
	defer f.Close()
	dec := toml.NewDecoder(f)
	if err := dec.Decode(config); err != nil {
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

func listenUnix(path string) (net.Listener, error) {
	stat, statErr := os.Stat(path)
	if os.IsNotExist(statErr) {
		return net.Listen("unix", path)
	} else if statErr != nil {
		return nil, statErr
	}
	// Socket still exists, clean up.
	if stat.Mode()|os.ModeSocket == 0 {
		return nil, fmt.Errorf("existing file is not a socket: %s", path)
	}
	if err := os.Remove(path); err != nil {
		return nil, fmt.Errorf("failed to remove socket: %w", err)
	}
	return net.Listen("unix", path)
}

func openDB() (*sqlx.DB, error) {
	// Force Go-compatible time handling.
	cfg, err := mysql.ParseDSN(viper.GetString(ConfMySQLDSN))
	if err != nil {
		return nil, err
	}
	cfg.ParseTime = true
	cfg.Loc = time.Local
	log.Info("Connecting to MySQL DB",
		zap.String("mysql.net", cfg.Net),
		zap.String("mysql.addr", cfg.Addr),
		zap.String("mysql.db_name", cfg.DBName),
		zap.String("mysql.user", cfg.User))
	// Connect
	return sqlx.Open("mysql", cfg.FormatDSN())
}
