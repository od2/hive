package topology

import (
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
)

// Config holds the full topology configuration of a Hive cluster.
type Config struct {
	Collections       []*Collection
	RedisShardFactory *RedisShardFactory
}

type Shard struct {
	Collection string
	Partition  int32
}

// GetCollection finds a collection by name.
// Returns nil if the collection does not exist.
func (c *Config) GetCollection(name string) *Collection {
	for _, cc := range c.Collections {
		if cc.Name == name {
			return cc
		}
	}
	return nil
}

// Collection holds the configuration specific to an item collection.
type Collection struct {
	Name   string
	PKType string

	// NAssign algorithm
	TaskAssignments uint          // assignments per task
	AssignInterval  time.Duration // assign/flush interval
	AssignBatch     uint          // size of message to distribute to workers
	// Session tracking
	SessionTimeout         time.Duration // session TTL, i.e. time until a session without heart beats gets dropped
	SessionRefreshInterval time.Duration // refresh session interval
	SessionExpireInterval  time.Duration // max session expire interval
	SessionExpireBatch     uint          // max sessions to expire at once
	// Event streaming
	TaskTimeout        time.Duration // in-flight assignment TTL, i.e. time given to worker to complete each task
	TaskExpireInterval time.Duration // max task expire interval (runs sooner by default) // TODO unused
	TaskExpireBatch    uint          // max assignments to expire at once
	DeliverBatch       uint          // max assignments in one gRPC server-side event
	// Result reporting
	ResultInterval time.Duration
	ResultBatch    uint
	ResultBackoff  time.Duration
}

var defaultCollection = Collection{
	TaskAssignments:        3,
	AssignInterval:         250 * time.Millisecond,
	AssignBatch:            1024,
	SessionTimeout:         5 * time.Minute,
	SessionRefreshInterval: 3 * time.Second,
	SessionExpireInterval:  10 * time.Second,
	SessionExpireBatch:     16,
	TaskTimeout:            time.Minute,
	TaskExpireInterval:     2 * time.Second,
	TaskExpireBatch:        128,
	DeliverBatch:           1024,
}

// Init populates the default options.
func (c *Collection) Init() {
	*c = defaultCollection
}

// RedisShardFactory manages Redis shards.
//
// As of now, it only supports the "Standalone" factory.
type RedisShardFactory struct {
	Type       string
	Standalone RedisShardFactoryStandalone
}

// RedisShardFactoryStandalone allocates all
// Redis shards in a single, standalone Redis database.
type RedisShardFactoryStandalone struct {
	Redis  redis.Options
	Client *redis.Client // Existing client override
}

// CollectionName returns a Kafka topic on a collection.
func CollectionTopic(collectionName, kind string) string {
	return collectionName + "." + kind
}

// CollectionOfTopic returns the collection of a Kafka topic.
// Returns an empty string on failure.
func CollectionOfTopic(topic string) string {
	i := strings.LastIndexByte(topic, '.')
	if i < 0 {
		return ""
	}
	return topic[:i]
}

// Kafka topic kinds for collections.
const (
	TopicCollectionTasks      = "tasks"
	TopicCollectionDiscovered = "discovered"
	TopicCollectionResults    = "results"
)
