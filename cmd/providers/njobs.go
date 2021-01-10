package providers

import (
	"context"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/spf13/viper"
	"go.od2.network/hive/pkg/njobs"
	"go.uber.org/zap"
)

const (
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

	ConfKafkaTopic     = "kafka.topic"
	ConfKafkaPartition = "kafka.partition"
)

func init() {
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

	viper.SetDefault(ConfKafkaTopic, "")
	viper.SetDefault(ConfKafkaPartition, int32(0))
}

func NewNJobsOptions() *njobs.Options {
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

type NJobsPartition struct {
	Topic     string
	Partition int32
}

func NewNJobsPartition(log *zap.Logger) *NJobsPartition {
	topic := viper.GetString(ConfKafkaTopic)
	partition := viper.GetInt32(ConfKafkaPartition)
	log.Info("Using Kafka parameters",
		zap.String(ConfKafkaTopic, topic),
		zap.Int32(ConfKafkaPartition, partition))
	if topic == "" {
		log.Fatal("Empty " + ConfKafkaTopic)
	}
	return &NJobsPartition{topic, partition}
}

func NewNJobsRedis(
	ctx context.Context,
	params *NJobsPartition,
	rd *redis.Client,
	opts *njobs.Options,
) (*njobs.RedisClient, error) {
	scripts, err := njobs.LoadScripts(ctx, rd)
	if err != nil {
		return nil, err
	}
	// Connect to Redis njobs.
	return &njobs.RedisClient{
		Redis:         rd,
		PartitionKeys: njobs.NewPartitionKeys(params.Topic, params.Partition),
		Scripts:       scripts,
		Options:       opts,
	}, nil
}
