package main

import (
	"time"

	"github.com/spf13/viper"
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
}
