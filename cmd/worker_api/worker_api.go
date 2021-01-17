package worker_api

import (
	"context"

	"github.com/Shopify/sarama"
	"github.com/spf13/cobra"
	"go.od2.network/hive/cmd/providers"
	"go.od2.network/hive/pkg/auth"
	"go.od2.network/hive/pkg/discovery"
	"go.od2.network/hive/pkg/njobs"
	"go.od2.network/hive/pkg/topology"
	"go.od2.network/hive/pkg/topology/redisshard"
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

func newDiscoveryServer(
	log *zap.Logger,
	server *grpc.Server,
	producer sarama.SyncProducer,
) {
	types.RegisterDiscoveryServer(server, &discovery.Handler{
		Producer: producer,
		Log:      log.Named("discovery"),
	})
}

func newAssignmentsServer(
	log *zap.Logger,
	server *grpc.Server,
	topology *topology.Config,
	factory redisshard.Factory,
) {
	streamer := njobs.Streamer{
		Topology: topology,
		Factory:  factory,
		Log:      log.Named("worker"),
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
