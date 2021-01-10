package main

import (
	"context"

	"github.com/Shopify/sarama"
	"github.com/spf13/cobra"
	"go.od2.network/hive/pkg/auth"
	"go.od2.network/hive/pkg/discovery"
	"go.od2.network/hive/pkg/njobs"
	"go.od2.network/hive/pkg/types"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

var workerAPICmd = cobra.Command{
	Use:   "worker-api",
	Short: "Run worker API server",
	Long: "Runs the gRPC server used to stream tasks to workers.\n" +
		"It is safe to load-balance multiple worker-api servers.",
	Args: cobra.NoArgs,
	Run: func(cmd *cobra.Command, _ []string) {
		app := fx.New(
			fx.Provide(providers),
			fx.Supply(cmd),
			fx.Invoke(runWorkerAPI),
			fx.Logger(zap.NewStdLog(log)),
		)
		app.Run()
	},
}

func init() {
	flags := workerAPICmd.Flags()
	flags.String("socket", "", "UNIX socket address")

	rootCmd.AddCommand(&workerAPICmd)
}

func runWorkerAPI(
	lc fx.Lifecycle,
	cmd *cobra.Command,
	interceptor *auth.WorkerAuthInterceptor,
	saramaConfig *sarama.Config,
	rc *njobs.RedisClient,
) {
	// Get flags
	flags := cmd.Flags()
	socket, err := flags.GetString("socket")
	if err != nil {
		panic(err)
	}
	server := grpc.NewServer(
		grpc.UnaryInterceptor(interceptor.Unary()),
		grpc.StreamInterceptor(interceptor.Stream()),
	)
	// Connect to Kafka.
	saramaClient := saramaClientFromEnv(saramaConfig)
	defer func() {
		if err := saramaClient.Close(); err != nil {
			log.Error("Failed to close sarama client", zap.Error(err))
		}
	}()
	log.Info("Creating Kafka producer")
	producer, err := sarama.NewSyncProducerFromClient(saramaClient)
	if err != nil {
		log.Fatal("Failed to build Kafka producer", zap.Error(err))
	}
	defer func() {
		log.Info("Closing Kafka producer")
		if err := producer.Close(); err != nil {
			log.Error("Failed to close Kafka producer", zap.Error(err))
		}
	}()
	streamer := njobs.Streamer{
		RedisClient: rc,
		Log:         log.Named("worker"),
	}
	types.RegisterAssignmentsServer(server, &streamer)
	types.RegisterDiscoveryServer(server, &discovery.Handler{
		Producer: producer,
		Log:      log.Named("discovery"),
	})
	// Start listener
	listen, err := listenUnix(socket)
	if err != nil {
		log.Fatal("Failed to listen", zap.String("socket", socket), zap.Error(err))
	}
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			go func() {
				log.Info("Starting server", zap.String("socket", socket))
				if err := server.Serve(listen); err != nil {
					log.Fatal("Server failed", zap.Error(err))
				}
			}()
			return nil
		},
		OnStop: func(ctx context.Context) error {
			server.Stop()
			return nil
		},
	})
}
