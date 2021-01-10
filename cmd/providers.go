package main

import (
	"context"
	"encoding/hex"

	"github.com/go-redis/redis/v8"
	"github.com/jmoiron/sqlx"
	"github.com/spf13/viper"
	"go.od2.network/hive/pkg/auth"
	"go.od2.network/hive/pkg/authgw"
	"go.od2.network/hive/pkg/njobs"
	"go.od2.network/hive/pkg/token"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

var providers = []interface{}{
	newRedis,
	newRedisNJobs,
	newMySQL,
	newAuthgwBackend,
	newAuthgwSigner,
	newWorkerAuthInterceptor,
}

func newRedis(ctx context.Context, lc fx.Lifecycle) (*redis.Client, error) {
	rd := redisClientFromEnv()
	if err := rd.Ping(ctx).Err(); err != nil {
		return nil, err
	}
	lc.Append(fx.Hook{
		OnStop: func(ctx context.Context) error {
			log.Info("Closing Redis client")
			err := rd.Close()
			if err != nil {
				log.Error("Failed to close Redis client", zap.Error(err))
			}
			return err
		},
	})
	return rd, nil
}

func newRedisNJobs(ctx context.Context, rd *redis.Client) (*njobs.RedisClient, error) {
	scripts, err := njobs.LoadScripts(ctx, rd)
	if err != nil {
		log.Fatal("Failed to load njobs scripts", zap.Error(err))
	}
	// Connect to Redis njobs.
	topic, partition := kafkaPartitionFromEnv()
	return &njobs.RedisClient{
		Redis:         rd,
		PartitionKeys: njobs.NewPartitionKeys(topic, partition),
		Scripts:       scripts,
		Options:       njobsOptionsFromEnv(),
	}, nil
}

func newMySQL(ctx context.Context, lc fx.Lifecycle) (*sqlx.DB, error) {
	db, err := openDB()
	if err != nil {
		return nil, err
	}
	if err := db.PingContext(ctx); err != nil {
		log.Fatal("Failed to ping DB", zap.Error(err))
	}
	lc.Append(fx.Hook{
		OnStop: func(ctx context.Context) error {
			return db.Close()
		},
	})
	return db, nil
}

func newAuthgwBackend(
	lc fx.Lifecycle,
	shutdown fx.Shutdowner,
	db *sqlx.DB,
	rd *redis.Client,
) (authgw.Backend, error) {
	// Build auth gateway.
	backend := authgw.Database{DB: db.DB}
	cachedBackend, err := authgw.NewCache(&backend,
		viper.GetInt(ConfAuthgwCacheSize),
		viper.GetDuration(ConfAuthgwCacheTTL))
	if err != nil {
		return nil, err
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
	innerCtx, cancel := context.WithCancel(context.Background())
	lc.Append(fx.Hook{
		OnStart: func(_ context.Context) error {
			go func() {
				if err := invalidation.Run(innerCtx); err != nil {
					log.Error("Auth cache invalidation failed", zap.Error(err))
					if err := shutdown.Shutdown(); err != nil {
						log.Fatal("Shutdown failed")
					}
				}
			}()
			return nil
		},
		OnStop: func(_ context.Context) error {
			cancel()
			return nil
		},
	})
	return cachedBackend, nil
}

func newAuthgwSigner() token.Signer {
	authSecret := new([32]byte)
	authSecretStr := viper.GetString(ConfAuthgwSecret)
	if len(authSecretStr) != 64 {
		log.Fatal("Invalid " + ConfAuthgwSecret)
	}
	if _, err := hex.Decode(authSecret[:], []byte(authSecretStr)); err != nil {
		log.Fatal("Invalid hex in " + ConfAuthgwSecret)
	}
	return token.NewSimpleSigner(authSecret)
}

func newWorkerAuthInterceptor(backend authgw.Backend, signer token.Signer) *auth.WorkerAuthInterceptor {
	return &auth.WorkerAuthInterceptor{
		Backend: backend,
		Signer:  signer,
		Log:     log.Named("auth"),
	}
}
