package main

import (
	"context"

	"github.com/jmoiron/sqlx"
	"github.com/spf13/cobra"
	"go.od2.network/hive/pkg/auth"
	"go.od2.network/hive/pkg/management"
	"go.od2.network/hive/pkg/token"
	"go.od2.network/hive/pkg/types"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

var managementAPICmd = cobra.Command{
	Use:   "management-api",
	Short: "Run management API server",
	Long: "Runs the gRPC server for the management API.\n" +
		"It is safe to load-balance multiple management-api servers.",
	Args: cobra.NoArgs,
	Run: func(cmd *cobra.Command, _ []string) {
		app := fx.New(
			fx.Provide(providers),
			fx.Supply(cmd),
			fx.Invoke(newManagementAPI),
			fx.Logger(zap.NewStdLog(log)),
		)
		app.Run()
	},
}

func init() {
	flags := managementAPICmd.Flags()
	flags.String("socket", "", "UNIX socket address")

	rootCmd.AddCommand(&managementAPICmd)
}

func newManagementAPI(
	lc fx.Lifecycle,
	cmd *cobra.Command,
	db *sqlx.DB,
	signer token.Signer,
) {
	// Get flags
	flags := cmd.Flags()
	socket, err := flags.GetString("socket")
	if err != nil {
		panic(err)
	}
	// Assemble server with web auth
	interceptor := auth.WebIdentityInterceptor{}
	server := grpc.NewServer(
		grpc.UnaryInterceptor(interceptor.Unary()),
		grpc.StreamInterceptor(interceptor.Stream()),
	)
	// Assemble handlers
	types.RegisterManagementServer(server, &management.Handler{
		DB:     db.DB,
		Signer: signer,
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
