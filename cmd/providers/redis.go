package providers

import (
	"context"

	"github.com/go-redis/redis/v8"
	"github.com/spf13/viper"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

const (
	ConfRedisNetwork = "redis.network"
	ConfRedisAddr    = "redis.addr"
	ConfRedisDB      = "redis.db"
)

func init() {
	viper.SetDefault(ConfRedisNetwork, "tcp")
	viper.SetDefault(ConfRedisAddr, "localhost:6379")
	viper.SetDefault(ConfRedisDB, 0)
}

func NewRedis(ctx context.Context, log *zap.Logger, lc fx.Lifecycle) (*redis.Client, error) {
	redisOpts := &redis.Options{
		Network: viper.GetString(ConfRedisNetwork),
		Addr:    viper.GetString(ConfRedisAddr),
		DB:      viper.GetInt(ConfRedisDB),
	}
	log.Info("Connecting to Redis",
		zap.String(ConfRedisNetwork, redisOpts.Network),
		zap.String(ConfRedisAddr, redisOpts.Addr),
		zap.Int(ConfRedisDB, redisOpts.DB))
	rd := redis.NewClient(redisOpts)
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
