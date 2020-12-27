package authgw

import (
	"context"
	"encoding/hex"
	"errors"
	"time"

	"github.com/go-redis/redis/v8"
	lru "github.com/hashicorp/golang-lru"
	"go.od2.network/hive/pkg/token"
)

// Cache is a local in-memory caching layer.
// Entries are invalidated through a Pub/Sub mechanism.
type Cache struct {
	Backend Backend
	Cache   *lru.Cache
	TTL     time.Duration
}

type cacheEntry struct {
	TokenInfo
	lastUpdated time.Time
}

// Notification instructs the deletion of a token entries from the cache.
type Notification []token.ID

// NewCache creates a new caching layer that keeps the number of entries specified.
func NewCache(backend Backend, cacheSize int, ttl time.Duration) (*Cache, error) {
	cache, err := lru.New(cacheSize)
	if err != nil {
		return nil, err
	}
	return &Cache{
		Backend: backend,
		Cache:   cache,
		TTL:     ttl,
	}, nil
}

// LookupToken consults the in-memory cache for a token.
// If the cache is missed, it falls back to LookupSlow.
func (c *Cache) LookupToken(ctx context.Context, id token.ID) (*TokenInfo, error) {
	// Get entry from cache.
	entryI, ok := c.Cache.Get(id)
	if ok {
		// Fast path: Read from cache.
		entry := entryI.(*cacheEntry)
		now := time.Now()
		// Check if cache entry has expired.
		if now.Sub(entry.lastUpdated) > c.TTL {
			c.Cache.Remove(id)
			c.GC() // Also prune other expired entries while we're at it.
			return c.LookupSlow(ctx, id)
		}
		// Return result.
		return &entry.TokenInfo, nil
	}
	// Slow path: Read from backend.
	return c.LookupSlow(ctx, id)
}

// LookupSlow reads from the underlying backend, writes to the cache and returns.
func (c *Cache) LookupSlow(ctx context.Context, id token.ID) (*TokenInfo, error) {
	res, err := c.Backend.LookupToken(ctx, id)
	if err != nil {
		return nil, err
	}
	c.Cache.Add(id, &cacheEntry{
		TokenInfo:   *res,
		lastUpdated: time.Now(),
	})
	return res, nil
}

// GC removes all expired entries.
func (c *Cache) GC() {
	now := time.Now()
	for {
		hash, entryI, ok := c.Cache.GetOldest()
		if !ok {
			break
		}
		entry := entryI.(*cacheEntry)
		if now.Sub(entry.lastUpdated) <= c.TTL {
			break
		}
		c.Cache.Remove(hash)
	}
}

// Assert Cache implements Backend.
var _ Backend = (*Cache)(nil)

// CacheInvalidation watches Redis for auth cache invalidations.
type CacheInvalidation struct {
	Cache *Cache
	Redis *redis.Client

	StreamKey string // Redis key
	Backlog   int64  // Number of invalidations to keep

	streamID string // ID of last message
}

// Run applies cache invalidations from Redis Streams.
func (i *CacheInvalidation) Run(ctx context.Context) error {
	for {
		if err := i.read(ctx); err != nil {
			return err
		}
	}
}

func (i *CacheInvalidation) read(ctx context.Context) error {
	if i.streamID == "" {
		i.streamID = "0"
	}
	streams, err := i.Redis.XRead(ctx, &redis.XReadArgs{
		Streams: []string{i.StreamKey, i.streamID},
		Count:   128,
		Block:   time.Second,
	}).Result()
	if errors.Is(err, redis.Nil) {
		return nil
	} else if err != nil {
		return err
	}
	if len(streams) < 1 {
		return nil
	}
	for _, msg := range streams[0].Messages {
		i.streamID = msg.ID
		idStr, ok := msg.Values["id"].(string)
		if !ok {
			continue
		}
		if len(idStr) != 24 {
			continue
		}
		var id token.ID
		if _, err := hex.Decode(id[:], []byte(idStr)); err != nil {
			continue
		}
		copy(id[:], idStr)
		i.Cache.Cache.Remove(id)
	}
	return nil
}

// Add commits another cache invalidation.
func (i *CacheInvalidation) Add(ctx context.Context, id token.ID) error {
	return i.Redis.XAdd(ctx, &redis.XAddArgs{
		Stream:       i.StreamKey,
		MaxLenApprox: i.Backlog,
		ID:           "*",
		Values:       []string{"id", hex.EncodeToString(id[:])},
	}).Err()
}
