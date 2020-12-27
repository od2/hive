package main

import (
	"context"
	"net"

	"github.com/spf13/cobra"
	"go.od2.network/hive/pkg/njobs"
	"go.od2.network/hive/pkg/types"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

var workerAPICmd = cobra.Command{
	Use:   "worker-api",
	Short: "Run worker API server",
	Long: "Runs the gRPC server used to stream tasks to workers.\n" +
		"It is safe to load-balance multiple worker-api servers.",
	Args: cobra.NoArgs,
	Run:  runWorkerAPI,
}

func init() {
	flags := workerAPICmd.Flags()
	flags.String("bind", ":8000", "Server bind")

	rootCmd.AddCommand(&workerAPICmd)
}

func runWorkerAPI(cmd *cobra.Command, _ []string) {
	// Get flags
	flags := cmd.Flags()
	bind, err := flags.GetString("bind")
	if err != nil {
		panic(err)
	}
	// Build handler
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rd := redisClientFromEnv()
	scripts, err := njobs.LoadScripts(ctx, rd)
	if err != nil {
		log.Fatal("Failed to load njobs scripts", zap.Error(err))
	}
	topic, partition := kafkaPartitionFromEnv()
	rc := njobs.RedisClient{
		Redis:         rd,
		PartitionKeys: njobs.NewPartitionKeys(topic, partition),
		Scripts:       scripts,
		Options:       njobsOptionsFromEnv(),
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
