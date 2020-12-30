package cachegc

import (
	"time"

	"github.com/hashicorp/golang-lru/simplelru"
)

// Cache is a local in-memory caching layer.
type Cache struct {
	simplelru.LRUCache
	TTL time.Duration
}

type cacheEntry struct {
	data        interface{}
	lastUpdated time.Time
}

// NewCache creates a new caching layer that keeps the number of entries specified.
func NewCache(cache simplelru.LRUCache, ttl time.Duration) *Cache {
	return &Cache{LRUCache: cache, TTL: ttl}
}

// Get returns an item in the cache, ignoring expired items.
func (c *Cache) Get(key interface{}) (value interface{}, ok bool) {
	entryI, ok := c.LRUCache.Get(key)
	if ok {
		entry := entryI.(*cacheEntry)
		now := time.Now()
		if now.Sub(entry.lastUpdated) > c.TTL {
			c.LRUCache.Remove(key)
			c.GetOldest() // trigger GC
			return nil, false
		}
		return entry.data, true
	}
	return nil, false
}

// GetOldest gets the oldest item that is not expired.
func (c *Cache) GetOldest() (interface{}, interface{}, bool) {
	now := time.Now()
	for {
		key, entryI, ok := c.LRUCache.GetOldest()
		if !ok {
			return nil, nil, false
		}
		entry := entryI.(*cacheEntry)
		if now.Sub(entry.lastUpdated) <= c.TTL {
			return key, entry.data, true
		}
		c.LRUCache.Remove(key)
	}
}

// TODO Implement RemoveOldest and Peek
