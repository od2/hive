package providers

import (
	"encoding/hex"
	"time"

	lru "github.com/hashicorp/golang-lru"
	"github.com/jmoiron/sqlx"
	"github.com/spf13/viper"
	"go.od2.network/hive/pkg/auth"
	"go.od2.network/hive/pkg/authgw"
	"go.od2.network/hive/pkg/cachegc"
	"go.od2.network/hive/pkg/token"
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

// NewAuthgwBackend returns a worker auth gateway connected to the provided SQL DB.
func NewAuthgwBackend(db *sqlx.DB) (authgw.Backend, error) {
	// Build auth gateway.
	backend := authgw.Database{DB: db.DB}
	cachedBackend, err := authgw.NewCache(&backend,
		viper.GetInt(ConfAuthgwCacheSize),
		viper.GetDuration(ConfAuthgwCacheTTL))
	if err != nil {
		return nil, err
	}
	// TODO Cache invalidation
	return cachedBackend, nil
}

// NewAuthgwSigner creates a worker token signer from config.
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

// NewWorkerAuthInterceptor creates a gRPC server auth interceptor according to config.
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

// AuthgwCache wraps cachegc.Cache for dependency injection.
type AuthgwCache struct {
	*cachegc.Cache
}

// NewAuthgwCache creates an auth LRU cache according to config.
func NewAuthgwCache() AuthgwCache {
	baseCache, err := lru.New(viper.GetInt(ConfAuthgwCacheSize))
	if err != nil {
		panic("failed to create cache: " + err.Error())
	}
	cache := cachegc.NewCache(baseCache, viper.GetDuration(ConfAuthgwCacheTTL))
	return AuthgwCache{cache}
}
