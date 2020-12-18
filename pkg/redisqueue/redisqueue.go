// Package redisqueue provides components to run a lightweight job queue on top of Redis.
//
// Components
//
// Redis 6 or newer is required.
// Apart form clients, at least one ExpirationWorker needs to run in the background to maintain the queue.
// The algorithms used heavily rely on Redis Lua server-side scripting for safe concurrent access.
//
// Properties
//
// RedisQueue holds a set of task IDs pushed by producers.
// Consumers claim random task IDs for processing with a static claim expiration time.
// Eventually claims finish and the task ID gets removed from the queue.
// This happens either by consumer completion (ack), rejection (n-ack) or expiration (timeout).
//
// Data structures
//
// The set of pending tasks is stored in an unsorted set.
// When a task is claimed, claim data is associated to the task in a hash map,
// and a future expiration event is stored on a list queue.
//
// Motivation
//
// Why implement a new job queue if there are already great projects like Celery, RabbitMQ, Kafka and Redis Streams?
//
// We thought our own queue on top of Redis is the best options all things considered:
// (1) We want to keep architecture complexity low by reusing deployments where we can and we already rely on Redis.
// (2) We want high throughput and low latency with small queues.
// (3) Reliable delivery, acknowledgement and dead-lettering is required.
// (4) Communication to od2 hornets (workers) is stateless (gRPC) which is incompatible with some protocols (e.g. AMQP).
// (5) We expect to rely on linear write ordering across certain components. Redis is single-threaded, so fits well.
// (6) Scaling regardless of consumer count: This queue behaves the same with 100k or one consumer IDs.
package redisqueue

// Keys holds the Redis keys used.
type Keys struct {
	PendingHash  string // pending and in-flight tasks
	InflightHash string // mapping from task IDs (inflight only)
	ExpireList   string // list of task IDs with expiration time (inflight only)
}

// KeysForPrefix creates Keys with a common prefix.
func KeysForPrefix(prefix string) Keys {
	return Keys{
		PendingHash:  prefix + "_P",
		InflightHash: prefix + "_I",
		ExpireList:   prefix + "_E",
	}
}
