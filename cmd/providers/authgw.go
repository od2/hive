package providers

import (
	"context"
	"encoding/hex"
	"time"

	"github.com/go-redis/redis/v8"
	lru "github.com/hashicorp/golang-lru"
	"github.com/jmoiron/sqlx"
	"github.com/spf13/viper"
	"go.od2.network/hive/pkg/auth"
	"go.od2.network/hive/pkg/authgw"
	"go.od2.network/hive/pkg/cachegc"
	"go.od2.network/hive/pkg/token"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

// Worker auth gateway config.
const (
	ConfAuthgwSecret         = "authgw.secret"
	ConfAuthgwCacheSize      = "authgw.cache.size"
	ConfAuthgwCacheTTL       = "authgw.cache.ttl"
	ConfAuthgwCacheStreamKey = "authgw.cache.stream_key"
	ConfAuthgwCacheBacklog   = "authgw.cache.backlog"
)

func init() {
	viper.SetDefault(ConfAuthgwSecret, "")
	viper.SetDefault(ConfAuthgwCacheSize, 1024)
	viper.SetDefault(ConfAuthgwCacheTTL, time.Hour)
	viper.SetDefault(ConfAuthgwCacheStreamKey, "token-invalidations")
	viper.SetDefault(ConfAuthgwCacheBacklog, 64)
}

func NewAuthgwBackend(
	lc fx.Lifecycle,
	shutdown fx.Shutdowner,
	db *sqlx.DB,
	rd *redis.Client,
	log *zap.Logger,
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

func NewAuthgwSigner(log *zap.Logger) token.Signer {
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

func NewWorkerAuthInterceptor(
	log *zap.Logger,
	backend authgw.Backend,
	signer token.Signer,
) *auth.WorkerAuthInterceptor {
	return &auth.WorkerAuthInterceptor{
		Backend: backend,
		Signer:  signer,
		Log:     log.Named("auth"),
	}
}

type AuthgwCache struct {
	*cachegc.Cache
}

func NewAuthgwCache() AuthgwCache {
	baseCache, err := lru.New(viper.GetInt(ConfAuthgwCacheSize))
	if err != nil {
		panic("failed to create cache: " + err.Error())
	}
	cache := cachegc.NewCache(baseCache, viper.GetDuration(ConfAuthgwCacheTTL))
	return AuthgwCache{cache}
}
