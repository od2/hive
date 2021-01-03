package main

import (
	"github.com/spf13/cobra"
	"go.od2.network/hive/pkg/auth"
	"go.od2.network/hive/pkg/management"
	"go.od2.network/hive/pkg/types"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

var managementAPICmd = cobra.Command{
	Use:   "management-api",
	Short: "Run management API server",
	Long: "Runs the gRPC server for the management API.\n" +
		"It is safe to load-balance multiple management-api servers.",
	Args: cobra.NoArgs,
	Run:  runManagementAPI,
}

func init() {
	flags := managementAPICmd.Flags()
	flags.String("socket", "", "UNIX socket address")

	rootCmd.AddCommand(&managementAPICmd)
}

func runManagementAPI(cmd *cobra.Command, _ []string) {
	// Get flags
	flags := cmd.Flags()
	socket, err := flags.GetString("socket")
	if err != nil {
		panic(err)
	}
	// Connect to SQL.
	log.Info("Connecting to MySQL")
	db, err := openDB()
	if err != nil {
		log.Fatal("Failed to connect to MySQL", zap.Error(err))
	}
	defer func() {
		log.Info("Closing MySQL client")
		if err := db.Close(); err != nil {
			log.Error("Failed to MySQL client", zap.Error(err))
		}
	}()
	if err := db.Ping(); err != nil {
		log.Fatal("Failed to ping DB", zap.Error(err))
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
		Signer: getSigner(),
	})
	// Start listener
	listen, err := listenUnix(socket)
	if err != nil {
		log.Fatal("Failed to listen", zap.String("socket", socket), zap.Error(err))
	}
	log.Info("Starting server", zap.String("socket", socket))
	if err := server.Serve(listen); err != nil {
		log.Fatal("Server failed", zap.Error(err))
	}
}
