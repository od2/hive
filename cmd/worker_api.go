package main

import (
	"context"
	"database/sql"
	"encoding/hex"
	"net"
	"sync"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.od2.network/hive/pkg/appctx"
	"go.od2.network/hive/pkg/authgw"
	"go.od2.network/hive/pkg/njobs"
	"go.od2.network/hive/pkg/token"
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
	// Connect to Redis.
	ctx, cancel := context.WithCancel(appctx.Context())
	defer cancel()
	rd := redisClientFromEnv()
	scripts, err := njobs.LoadScripts(ctx, rd)
	if err != nil {
		log.Fatal("Failed to load njobs scripts", zap.Error(err))
	}
	// Connect to Redis njobs.
	topic, partition := kafkaPartitionFromEnv()
	rc := njobs.RedisClient{
		Redis:         rd,
		PartitionKeys: njobs.NewPartitionKeys(topic, partition),
		Scripts:       scripts,
		Options:       njobsOptionsFromEnv(),
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
	// Build auth gateway.
	backend := authgw.Database{DB: db}
	cachedBackend, err := authgw.NewCache(&backend,
		viper.GetInt(ConfAuthgwCacheSize),
		viper.GetDuration(ConfAuthgwCacheTTL))
	if err != nil {
		log.Fatal("Failed to build auth gateway cache", zap.Error(err))
	}
	invalidation := authgw.CacheInvalidation{
		Cache:     cachedBackend,
		Redis:     rd,
		StreamKey: viper.GetString(ConfAuthgwCacheStreamKey),
		Backlog:   0,
	}
	if invalidation.StreamKey == "" {
		log.Fatal("Missing " + ConfAuthgwCacheStreamKey)
	}
	log.Info("Starting auth cache invalidator")
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := invalidation.Run(ctx); err != nil {
			log.Fatal("Auth cache invalidation failed", zap.Error(err))
		}
	}()
	authSecret := new([32]byte)
	authSecretStr := viper.GetString(ConfAuthgwSecret)
	if len(authSecretStr) != 64 {
		log.Fatal("Invalid " + ConfAuthgwSecret)
	}
	if _, err := hex.Decode(authSecret[:], []byte(authSecretStr)); err != nil {
		log.Fatal("Invalid hex in " + ConfAuthgwSecret)
	}
	signer := token.NewSimpleSigner(authSecret)
	interceptor := authgw.Interceptor{
		Backend: cachedBackend,
		Signer:  signer,
	}
	server := grpc.NewServer(
		grpc.UnaryInterceptor(interceptor.Unary()),
		grpc.StreamInterceptor(interceptor.Stream()),
	)
	streamer := njobs.Streamer{
		RedisClient: &rc,
	}
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
	wg.Wait()
}
