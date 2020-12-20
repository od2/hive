// Package njobs forms a job queue for distributing tasks among a lot of workers.
//
// It works on messages from Kafka and stores metadata for each partition in Redis.
//
// Tasks can be configured to be processed by N distinct workers.
// The queue is somewhat hardened against algorithmic complexity attacks.
//
// Usage
//
// The queue needs a few background workers to operate (mainly to evict old entries).
// The in-memory queue is typically filled from Kafka and indirectly exposed to workers through an API.
//
// Workers will establish one or more worker sessions to read jobs and commit them.
// Each worker session, so it is limited by the throughput of a single partition.
//
// The Kafka keys of messages must be binary big-endian encodings of the int64 item ID.
//
// You must ensure that worker IDs, Kafka offsets, unix epochs and item IDs
// do not exceed 2^53 because of limitations in Lua 5.1.
// Newer versions of Lua do not have this problem, but unfortunately Redis chose to not upgrade.
//
// All exported components are thread-safe internally and ready for cross-component concurrency.
// e.g. When doing a Kubernetes rolling upgrade it's fine to have more than one routine access the same resources.
//
// Architecture
//
// njobs forms a Kafka consumer group with one "Assigner" routine for each consumer/partition.
//
// The assignment process per partition is strictly single-threaded.
// To maximize performance and minimize I/O it relies heavily on Redis server-side Lua scripting.
// In fact, the entire matching algorithm is executed server-side on Redis.
//
// A single partition / assigner should be able to keep up with thousands of items per second.
// Increasing processor speed (Redis), storage throughput (Kafka), and batch sizes achieves vertical scaling.
// Horizontal scaling is achieved by Kafka partitions and Redis instances.
// Multiple Redis instances need to run independently (incompatible with Redis Cluster).
//
// Persistence
//
// The components in this package rely on Kafka and Redis to store state and hold small amounts of dirty cache.
// Data loss in Kafka or Redis results in old tasks being processed more than once.
// Per design, it is not possible to skip tasks.
package njobs

import "time"

// Options stores global settings.
//
// TODO WorkerQoS should be dynamic per worker
type Options struct {
	N              uint          // assignments per task
	TaskTimeout    time.Duration // in-flight assignment TTL, i.e. time given to worker to complete each task
	WorkerQoS      uint          // max in-flight tasks per worker
	AssignInterval time.Duration // assign/flush interval
	AssignBatch    uint          // size of message to distribute to workers
	ExpireInterval time.Duration // max expire interval (runs sooner by default)
	ExpireBatch    uint          // max assignments to expire at once
}
