package worker_api

import (
	"context"

	"github.com/Shopify/sarama"
	"github.com/spf13/cobra"
	"go.od2.network/hive/cmd/providers"
	"go.od2.network/hive/pkg/auth"
	"go.od2.network/hive/pkg/discovery"
	"go.od2.network/hive/pkg/njobs"
	"go.od2.network/hive/pkg/types"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

var Cmd = cobra.Command{
	Use:   "worker-api",
	Short: "Run worker API server",
	Long: "Runs the gRPC server used to stream tasks to workers.\n" +
		"It is safe to load-balance multiple worker-api servers.",
	Args: cobra.NoArgs,
	Run: func(cmd *cobra.Command, _ []string) {
		app := providers.NewApp(
			cmd,
			fx.Provide(
				newWorkerAPIFlags,
				runWorkerAPI,
				fx.Annotated{
					Name:   "worker_api_producer",
					Target: newWorkerAPIProducer,
				},
			),
			fx.Invoke(
				newDiscoveryServer,
				newAssignmentsServer,
			),
		)
		app.Run()
	},
}

func init() {
	flags := Cmd.Flags()
	flags.String("socket", "", "UNIX socket address")
}

type workerAPIFlags struct {
	socket string
}

func newWorkerAPIFlags(cmd *cobra.Command) *workerAPIFlags {
	flags := cmd.Flags()
	socket, err := flags.GetString("socket")
	if err != nil {
		panic(err)
	}
	return &workerAPIFlags{
		socket: socket,
	}
}

func newWorkerAPIProducer(
	lc fx.Lifecycle,
	log *zap.Logger,
	saramaClient sarama.Client,
) (sarama.SyncProducer, error) {
	log.Info("Creating Kafka producer")
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

type discoveryServerIn struct {
	fx.In

	Server   *grpc.Server
	Producer sarama.SyncProducer `name:"worker_api_producer"`
}

func newDiscoveryServer(log *zap.Logger, inputs *discoveryServerIn) {
	types.RegisterDiscoveryServer(inputs.Server, &discovery.Handler{
		Producer: inputs.Producer,
		Log:      log.Named("discovery"),
	})
}

func newAssignmentsServer(
	log *zap.Logger,
	server *grpc.Server,
	rc *njobs.RedisClient,
) {
	streamer := njobs.Streamer{
		RedisClient: rc,
		Log:         log.Named("worker"),
	}
	types.RegisterAssignmentsServer(server, &streamer)
}

func runWorkerAPI(
	lc fx.Lifecycle,
	log *zap.Logger,
	flags *workerAPIFlags,
	interceptor *auth.WorkerAuthInterceptor,
) (*grpc.Server, error) {
	// Get flags
	server := grpc.NewServer(
		grpc.UnaryInterceptor(interceptor.Unary()),
		grpc.StreamInterceptor(interceptor.Stream()),
	)
	// Start listener
	listen, err := providers.ListenUnix(flags.socket)
	if err != nil {
		return nil, err
	}
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			go func() {
				log.Info("Starting server", zap.String("socket", flags.socket))
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
	return server, nil
}
