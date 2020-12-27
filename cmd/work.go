package main

import (
	"context"
	"net"

	"github.com/go-redis/redis/v8"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.od2.network/hive/pkg/njobs"
	"go.od2.network/hive/pkg/types"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

var workCmd = cobra.Command{
	Use:   "work",
	Short: "Run work server",
	Args:  cobra.NoArgs,
	Run:   runWork,
}

func init() {
	flags := workCmd.Flags()
	flags.String("bind", ":8000", "Server bind")
}

func runWork(cmd *cobra.Command, _ []string) {
	// Get flags
	flags := cmd.Flags()
	bind, err := flags.GetString("bind")
	if err != nil {
		panic(err)
	}
	// Build handler
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	redisOpts := &redis.Options{
		Network: viper.GetString(ConfRedisNetwork),
		Addr:    viper.GetString(ConfRedisAddr),
		DB:      viper.GetInt(ConfRedisDB),
	}
	log.Info("Connecting to Redis",
		zap.String(ConfRedisNetwork, redisOpts.Network),
		zap.String(ConfRedisAddr, redisOpts.Addr),
		zap.Int(ConfRedisDB, redisOpts.DB))
	rd := redis.NewClient(redisOpts)
	scripts, err := njobs.LoadScripts(ctx, rd)
	if err != nil {
		log.Fatal("Failed to load njobs scripts", zap.Error(err))
	}
	topic := viper.GetString(ConfKafkaTopic)
	partition := viper.GetInt32(ConfKafkaPartition)
	log.Info("Using Kafka parameters",
		zap.String(ConfKafkaTopic, topic),
		zap.Int32(ConfKafkaPartition, partition))
	rc := njobs.RedisClient{
		Redis:         rd,
		PartitionKeys: njobs.NewPartitionKeys(topic, partition),
		Scripts:       scripts,
		Options: &njobs.Options{
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
		},
	}
	streamer := njobs.Streamer{
		RedisClient: &rc,
	}
	server := grpc.NewServer()
	types.RegisterAssignmentsServer(server, &streamer)
	// Start listener
	listen, err := net.Listen("tcp", bind)
	if err != nil {
		log.Fatal("Failed to listen", zap.String("bind", bind), zap.Error(err))
	}
	log.Info("Starting server", zap.String("bind", bind))
	if err := server.Serve(listen); err != nil {
		log.Fatal("Server failed", zap.Error(err))
	}
}
