package worker_api

import (
	"github.com/Shopify/sarama"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
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
			fx.Provide(Server),
			fx.Invoke(
				NewDiscoveryServer,
				NewAssignmentsServer,
			),
		)
		app.Run()
	},
}

// Management API config.
const (
	ConfListenNet  = "worker_api.listen.net"
	ConfListenAddr = "worker_api.listen.addr"
)

func init() {
	viper.SetDefault(ConfListenNet, "tcp")
	viper.SetDefault(ConfListenAddr, "localhost:7700")
}

func NewDiscoveryServer(
	log *zap.Logger,
	server *grpc.Server,
	producer sarama.SyncProducer,
) {
	types.RegisterDiscoveryServer(server, &discovery.Handler{
		Producer: producer,
		Log:      log.Named("discovery"),
	})
}

func NewAssignmentsServer(
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

func Server(
	lc fx.Lifecycle,
	log *zap.Logger,
	interceptor *auth.WorkerAuthInterceptor,
) (*grpc.Server, error) {
	// Get flags
	server := grpc.NewServer(
		grpc.UnaryInterceptor(interceptor.Unary()),
		grpc.StreamInterceptor(interceptor.Stream()),
	)
	// Start listener
	listen := providers.MustListen(log,
		viper.GetString(ConfListenNet),
		viper.GetString(ConfListenAddr))
	providers.LifecycleServe(log, lc, listen, server)
	return server, nil
}
