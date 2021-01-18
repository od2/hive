package topology

import (
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/pelletier/go-toml"
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
	TaskAssignments uint          `default:"3"`     // assignments per task
	AssignInterval  time.Duration `default:"250ms"` // assign/flush interval
	AssignBatch     uint          `default:"1024"`  // size of message to distribute to workers
	// Session tracking
	SessionTimeout         time.Duration `default:"5m"`  // session TTL, i.e. time until a session without heart beats gets dropped
	SessionRefreshInterval time.Duration `default:"3s"`  // refresh session interval
	SessionExpireInterval  time.Duration `default:"10s"` // max session expire interval
	SessionExpireBatch     uint          `default:"16"`  // max sessions to expire at once
	// Event streaming
	TaskTimeout     time.Duration `default:"60s"`  // in-flight assignment TTL, i.e. time given to worker to complete each task
	TaskExpireBatch uint          `default:"128"`  // max assignments to expire at once
	DeliverBatch    uint          `default:"1024"` // max assignments in one gRPC server-side event
	// Result reporting
	ResultInterval time.Duration `default:"2s"`
	ResultBatch    uint          `default:"128"`
	ResultBackoff  time.Duration `default:"10s"`
}

// Init populates the default options.
func (c *Collection) Init() {
	if err := toml.Unmarshal([]byte(``), c); err != nil {
		panic(err.Error())
	}
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
