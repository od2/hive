package redisshard

import (
	"fmt"

	"github.com/go-redis/redis/v8"
	"go.od2.network/hive/pkg/topology"
)

// Factory creates Redis shards.
type Factory interface {
	GetShard(shard topology.Shard) (*redis.Client, error)
}

// NewFactory creates a new Redis shard factory from the topology config.
func NewFactory(c *topology.RedisShardFactory) (Factory, error) {
	switch c.Type {
	case "Standalone":
		f := NewStandaloneFactory(&c.Standalone)
		return f, nil
	default:
		return nil, fmt.Errorf("unknown Redis shard factory type: %s", c.Type)
	}
}

// StandaloneFactory points all Redis shards to the same Redis server.
type StandaloneFactory struct {
	Redis *redis.Client
}

// NewStandaloneFactory creates a client wrapper for a single Redis server.
func NewStandaloneFactory(c *topology.RedisShardFactoryStandalone) *StandaloneFactory {
	f := new(StandaloneFactory)
	if c.Client != nil {
		f.Redis = c.Client
	} else {
		f.Redis = redis.NewClient(&c.Redis)
	}
	return &StandaloneFactory{c.Client}
}

// GetShard always returns the same Redis client.
func (sf *StandaloneFactory) GetShard(_ topology.Shard) (*redis.Client, error) {
	return sf.Redis, nil
}
