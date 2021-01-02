package main

import (
	"database/sql"

	"github.com/Shopify/sarama"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.od2.network/hive/pkg/auth"
	"go.od2.network/hive/pkg/discovery"
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
	db, err := sql.Open("mysql", viper.GetString(ConfMySQLDSN))
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
	// Connect to Kafka.
	saramaClient := saramaClientFromEnv()
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
	// Assemble handlers
	types.RegisterManagementServer(server, &management.Handler{
		DB:     db,
		Signer: getSigner(),
	})
	types.RegisterDiscoveryServer(server, &discovery.Handler{
		Producer: producer,
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
