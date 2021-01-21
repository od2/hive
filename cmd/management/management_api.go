package management

import (
	"github.com/jmoiron/sqlx"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.od2.network/hive-api"
	"go.od2.network/hive/cmd/providers"
	"go.od2.network/hive/pkg/auth"
	"go.od2.network/hive/pkg/management"
	"go.od2.network/hive/pkg/token"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// Cmd is the management-api sub-command.
var Cmd = cobra.Command{
	Use:   "management-api",
	Short: "Run management API server",
	Long: "Runs the gRPC server for the management API.\n" +
		"It is safe to load-balance multiple management-api servers.",
	Args: cobra.NoArgs,
	Run: func(cmd *cobra.Command, _ []string) {
		app := providers.NewApp(fx.Invoke(Run))
		app.Run()
	},
}

// Management API config.
const (
	ConfListenNet  = "management.listen.net"
	ConfListenAddr = "management.listen.addr"
)

func init() {
	viper.SetDefault(ConfListenNet, "tcp")
	viper.SetDefault(ConfListenAddr, "localhost:7701")
}

// Run hooks the management-api service into the application lifecycle.
func Run(
	lc fx.Lifecycle,
	log *zap.Logger,
	db *sqlx.DB,
	signer token.Signer,
) {
	// Assemble server with web auth
	interceptor := auth.WebIdentityInterceptor{}
	server := grpc.NewServer(
		grpc.UnaryInterceptor(interceptor.Unary()),
		grpc.StreamInterceptor(interceptor.Stream()),
	)
	// Assemble handlers
	hive.RegisterManagementServer(server, &management.Handler{
		DB:     db.DB,
		Signer: signer,
	})
	// Start listener
	listen := providers.MustListen(log,
		viper.GetString(ConfListenNet),
		viper.GetString(ConfListenAddr))
	providers.LifecycleServe(log, lc, listen, server)
}
