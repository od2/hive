// Package njobs forms a job queue for distributing tasks among a lot of workers.
//
// It reads messages from Kafka, runs them through the N-Assign algorithm, and streams assignments to the workers.
//
// N-Assign
//
// N-Assign ensures that each task is processed by N distinct workers, via N task assignments.
// The algorithm is somewhat hardened against complexity attacks.
//
// There is server-side flow control to prevent workers from getting too much tasks assigned,
// as well as client-side flow control so workers can choose a task rate at which they are comfortable with.
//
// Sessions
//
// Workers will establish one or more worker sessions to read jobs and commit them.
// Each worker session has an assignment limit that gets decremented each time the server sends an assignment.
// Once it reaches zero, the server stops sending.
// Workers will negotiate with the server how much to increment the task limit asynchronously.
// The sum of all session task limits will not exceed the predefined worker assignment allowance.
//
// Sessions shut down automatically after they have been idle for a while,
// or when the client closes the session explicitly.
//
// TODO Allow binding worker sessions to multiple partitions.
//
// Input Queue
//
// The Kafka keys of messages must be binary big-endian encodings of the int64 item ID.
//
// You must ensure that worker IDs, Kafka offsets, unix epochs and item IDs
// do not exceed 2^53 because of limitations in Lua 5.1.
// Newer versions of Lua do not have this problem, but unfortunately Redis chose to not upgrade.
//
// The queue needs a few background workers to operate (mainly to evict old entries).
// The in-memory queue is typically filled from Kafka and indirectly exposed to workers through an API.
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
//
// All data traveling between nqueue components (Kafka, Redis, Go connectors) is covered by at-least-once semantics.
package njobs

import "time"

// Options stores global settings.
type Options struct {
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
}

// DefaultOptions returns the default njobs options.
// Only pass by value, not reference, to avoid modifying this globally.
var DefaultOptions = Options{
	// NAssign algorithm
	TaskAssignments: 3,
	AssignInterval:  250 * time.Millisecond,
	AssignBatch:     2048,
	// Session tracking
	SessionTimeout:         5 * time.Minute,
	SessionRefreshInterval: 3 * time.Second,
	SessionExpireInterval:  10 * time.Second,
	SessionExpireBatch:     16,
	// Event streaming
	TaskTimeout:        time.Minute,
	TaskExpireInterval: 2 * time.Second,
	TaskExpireBatch:    128,
	DeliverBatch:       2048,
}
