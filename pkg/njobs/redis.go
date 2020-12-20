package njobs

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"github.com/Shopify/sarama"
	"github.com/go-redis/redis/v8"
)

// TopicKeys specifies the Redis keys for topic-scoped information.
type TopicKeys struct {
	WorkerSessions string // No. active workers per session
}

// NewTopicKeys returns the default TopicKeys for a given Kafka topic.
func NewTopicKeys(topic string) TopicKeys {
	return TopicKeys{
		WorkerSessions: topicKey(topic, 'S'),
	}
}

func topicKey(topic string, field int8) string {
	return fmt.Sprintf("njobs_v0\x00%s\x00%c", topic, rune(field))
}

// PartitionKeys specifies the Redis keys for partition-scoped information.
type PartitionKeys struct {
	topic     string
	partition int32

	Progress      string // Int64 of Kafka partition offset (global worker offset)
	Assignments   string // Hash Map: message => no. of assignments
	ActiveWorkers string // Sorted Set of active worker offsets
	IdleWorkers   string // Hash Map: worker => (offset, mode)
	Inflight      string // Sorted Set: struct{worker, offset, item}
	Expirations   string // Sorted Set: struct{worker, offset, item} by exp_time
}

// NewPartitionKeys returns the default PartitionKeys for a given Kafka partition.
func NewPartitionKeys(topic string, partition int32) PartitionKeys {
	return PartitionKeys{
		topic: topic,
		partition: partition,

		Progress:      partitionKey(topic, partition, 'G'),
		ActiveWorkers: partitionKey(topic, partition, 'A'),
		IdleWorkers:   partitionKey(topic, partition, 'I'),
		Assignments:   partitionKey(topic, partition, 'M'),
	}
}

func partitionKey(topic string, partition int32, field int8) string {
	return fmt.Sprintf("njobs_v0\x00%s\x00%d\x00%c", topic, partition, rune(field))
}

// RedisClient interfaces with Redis to store njobs metadata.
type RedisClient struct {
	// Modules
	Redis *redis.Client
	// Settings
	*Options
	PartitionKeys PartitionKeys
	// Redis scripts
	*Scripts
}

// Scripts holds Redis Lua server-side scripts.
type Scripts struct {
	enableWorker *redis.Script
	assignTasks  *redis.Script
	expire       *redis.Script
	ack          *redis.Script
}

// LoadScripts hashes the Lua server-side scripts and pre-loads them into Redis.
func LoadScripts(ctx context.Context, r *redis.Client) (*Scripts, error) {
	s := new(Scripts)
	s.enableWorker = redis.NewScript(enableWorkerScript)
	if err := s.enableWorker.Load(ctx, r).Err(); err != nil {
		return nil, err
	}
	s.assignTasks = redis.NewScript(assignTasksScript)
	if err := s.assignTasks.Load(ctx, r).Err(); err != nil {
		return nil, err
	}
	s.expire = redis.NewScript(expireScript)
	if err := s.expire.Load(ctx, r).Err(); err != nil {
		return nil, err
	}
	s.ack = redis.NewScript(ackScript)
	if err := s.ack.Load(ctx, r).Err(); err != nil {
		return nil, err
	}
	return s, nil
}

// enableWorkerScript the state of a worker to active.
// Keys:
// 1. Hash Map inactive workers
// 2. Sorted Set active worker offsets
// Arguments:
// 1. Worker ID
const enableWorkerScript = `
local inactive_worker = redis.call("HGET", KEYS[1], ARGV[1])
local offset = 0
local stopped = 1
if not inactive_worker then
  offset, stopped = struct.unpack("lB", inactive_worker)
end
if stopped == 1 then
  redis.call("HDEL", KEYS[1], ARGV[1])
  redis.call("ZADD", KEYS[2], offset, ARGV[1])
end
`

// EvalEnableWorker moves a worker to the active set, giving it tasks.
func (r *RedisClient) EvalEnableWorker(ctx context.Context, worker int64) error {
	keys := []string{
		r.PartitionKeys.IdleWorkers,
		r.PartitionKeys.ActiveWorkers,
	}
	return r.enableWorker.Run(ctx, r.Redis, keys, worker).Err()
}

// assignTasksScript takes in a batch of tasks and distributes it across workers.
// Keys:
// 1. String topic offset
// 2. Hash Map message => tries
// 3. Sorted Set active worker offsets
// 4. Hash Map inactive workers
// 5. Sorted Set in-flight tasks
// 6. Sorted Set expiration queue
// Arguments:
// 1. Number of assignments per task
// 2. List of message offsets
// 3. Max pending assignments per worker
// 4. Assignment expiration time
// Returns list:
// 1. New Kafka consumer offset
// 2. List of (worker, offset) tuples
// 3. Status code
const assignTasksScript = `
-- Keys
local key_progress = KEYS[1]
local key_message_tries = KEYS[2]
local key_active_workers = KEYS[3]
local key_idle_workers = KEYS[4]
local key_inflight = KEYS[5]
local key_expiry = KEYS[6]
-- Arguments
local replicas = ARGV[1]
local batch = ARGV[2]
local max_pending = ARGV[3]
local exp_time = ARGV[4]

local assignments = {}
-- Loop through messages
local progress = redis.call("GET", key_progress) or 0
for i=1,#batch,2 do
  local offset = batch[i]
  local item = batch[i+1]
  if offset <= progress then
    -- Kafka consumer is behind Redis state.
    return {min_offset, assignments, "ERR_SEEK"}
  end
  -- Assign each message N times
  local tries = redis.call("HINCRBY", key_message_tries, offset, 1)
  for j=tries,replicas,1 do
    -- Assign task to worker with lowest progress, and update progress.
    local worker_p = redis.call("ZPOPMIN", key_active_workers)
    if #worker_p == 0 then
      return {min_offset, assignments, "ERR_NO_WORKERS"}
    end
    local worker = worker_p[1]
    redis.call("ZADD", key_active_workers, offset, worker)
    -- Register task in task set and expiration queue.
    local entry = struct.pack("lll", worker, offset, item)
    redis.call("ZADD", key_inflight, 0, entry)
    redis.call("ZADD", key_expiry, exp_time, entry)
    table.insert(assignments, worker)
    table.insert(assignments, offset)
    -- Pause worker if its queue is saturated.
    local worker_pending = redis.call("ZLEXCOUNT", key_inflight, 
      "[" .. struct.pack("ll", worker, 0),
      "(" .. struct.pack("ll", worker + 1, 0))
    if worker_pending >= max_pending then
      redis.call("ZREM", key_active_workers, worker)
      redis.call("HSET", key_idle_workers, workers, struct.pack("lB", offset, false))
    end
  end
  -- Move forward progress.
  progress = offset
  redis.call("HDEL", key_message_tries, offset)
  redis.call("SET", key_progress, offset)
end
return {offset, assignments, "OK"}
`

// Assignment marks an incoming Kafka message matched to a worker.
type Assignment struct {
	Worker int64
	*sarama.ConsumerMessage
}

// Assignment errors.
var (
	ErrNoWorkers = errors.New("no workers available")
	ErrSeek      = errors.New("Kafka/Redis seek mismatch")
)

// evalAssignTasks runs the assignTasks script.
// It returns the offset of the last message fully consumed and a list of assignments.
func (r *RedisClient) evalAssignTasks(ctx context.Context, batch []*sarama.ConsumerMessage) (int64, []Assignment, error) {
	expireAt := time.Now().Add(r.TaskTimeout).Unix()
	keys := []string{
		r.PartitionKeys.Progress,
		r.PartitionKeys.Assignments,
		r.PartitionKeys.ActiveWorkers,
		r.PartitionKeys.IdleWorkers,
	}
	tasks := make([]interface{}, len(batch))
	offsetsMap := make(map[int64]*sarama.ConsumerMessage)
	for i, msg := range batch {
		tasks[i*2] = msg.Offset
		// Decode Kafka key as big endian int64 item ID.
		if len(msg.Key) != 8 {
			return 0, nil, fmt.Errorf("invalid Kafka key: %x", msg.Key)
		}
		tasks[(i*2)+1] = int64(binary.BigEndian.Uint64(msg.Key))
		offsetsMap[msg.Offset] = msg
	}
	cmd := r.assignTasks.Run(ctx, r.Redis, keys,
		int64(r.N), tasks, int64(r.WorkerQoS), expireAt)
	res, err := cmd.Result()
	if err != nil {
		return 0, nil, err
	}
	resSlice, ok := res.([]interface{})
	if !ok {
		return 0, nil, fmt.Errorf("unexpected res type: %T", res)
	}
	if len(resSlice) != 3 {
		return 0, nil, fmt.Errorf("unexpected res len: %d", len(resSlice))
	}
	newOffset, ok := resSlice[0].(int64)
	if !ok {
		return 0, nil, fmt.Errorf("unexpected res[0] type: %T", resSlice[0])
	}
	assignmentsSlice, ok := resSlice[1].([]interface{})
	if !ok {
		return 0, nil, fmt.Errorf("unexpected res[1] type: %T", resSlice[1])
	}
	if len(assignmentsSlice)%2 != 0 {
		return 0, nil, fmt.Errorf("odd res[1] len: %d", len(assignmentsSlice))
	}
	assignments := make([]Assignment, len(assignmentsSlice)/2)
	status, ok := resSlice[2].(string)
	if !ok {
		return 0, nil, fmt.Errorf("unexpected res[2] type: %T", resSlice[2])
	}
	for i := 0; i < len(assignmentsSlice); i += 2 {
		worker, ok := assignmentsSlice[i].(int64)
		if !ok {
			return 0, nil, fmt.Errorf("unexpected res[1][%d] type: %T", i, assignmentsSlice[i])
		}
		offset, ok := assignmentsSlice[i+1].(int64)
		if !ok {
			return 0, nil, fmt.Errorf("unexpected res[1][%d] type: %T", i+1, assignmentsSlice[i+1])
		}
		msg := offsetsMap[offset]
		if msg == nil {
			return 0, nil, fmt.Errorf("no message for offset %d", offset)
		}
		assignmentsSlice[i/2] = Assignment{worker, msg}
	}
	var retErr error
	switch status {
	case "OK":
		retErr = nil
	case "ERR_SEEK":
		retErr = ErrSeek
	case "ERR_NO_WORKERS":
		retErr = ErrNoWorkers
	default:
		retErr = fmt.Errorf("unknown error code: %s", status)
	}
	return newOffset, assignments, retErr
}

// expireScript removes expired in-flight assignments.
// Keys:
// 1. Sorted Set expiration queue
// 2. Sorted Set in-flight tasks
// Arguments:
// 1. Current unix time
// 2. Max no. of items to expire this invocation
// Returns list:
// 1. Tuple (worker ID, Kafka message offset, item ID)
// 2. Next expiration time
const expireScript = `
-- Keys
local key_expiry = KEYS[1]
local key_inflight = KEYS[2]
-- Argument
local time = ARGV[1]
local limit = ARGV[2]

-- Remove expired items
local results = {}
local expired = redis.call("ZRANGEBYSCORE", key_expiry, "-inf", time-1, "LIMIT", 0, limit)
redis.call("ZREM", key_inflight, expired)
for expire in expired do
  local worker, offset, item = struct.unpack("lll", expire)
  table.insert(results, worker)
  table.insert(results, offset)
  table.insert(results, item)
  
end
-- Get time of next expiry
local next_expiry_entry = redis.call("ZRANGE", key_expire, 0, 0, "WITHSCORES")
local next_expiry = 0
if next_expiry_entry then
  next_expiry = next_expiry_entry[2]
end
return {results, next_expiry}
`

// Expiration marks a worker task assignment as expired.
type Expiration struct {
	Worker int64
	Offset int64 // Kafka
	ItemID int64
}

// evalExpire pops expired items and time to wait until next expiry.
// It processes at most "limit" messages at once.
// The "time to wait" is set to -1 when there are no more items.
func (r *RedisClient) evalExpire(ctx context.Context, limit uint) ([]Expiration, time.Duration, error) {
	nowUnix := time.Now().Unix()
	keys := []string{r.PartitionKeys.Expirations, r.PartitionKeys.Inflight}
	res, err := r.expire.Run(ctx, r.Redis, keys, nowUnix, int64(limit)).Result()
	if err != nil {
		return nil, 0, err
	}
	resSlice, ok := res.([]interface{})
	if !ok {
		return nil, 0, fmt.Errorf("unexpected res type: %T", res)
	}
	if len(resSlice) != 2 {
		return nil, 0, fmt.Errorf("unexpected res len: %d", len(resSlice))
	}
	expireSlice, ok := resSlice[0].([]interface{})
	if !ok {
		return nil, 0, fmt.Errorf("unexpected res[0] type: %T", resSlice[0])
	}
	if len(expireSlice)%3 != 0 {
		return nil, 0, fmt.Errorf("odd res[0] len: %d", len(expireSlice))
	}
	nextExpiry, ok := resSlice[1].(int64)
	if !ok {
		return nil, 0, fmt.Errorf("unexpected res[1] type: %T", resSlice[1])
	}
	expirations := make([]Expiration, len(expireSlice)/3)
	for i := 0; i < len(expireSlice); i += 3 {
		worker, ok := expireSlice[i].(int64)
		if !ok {
			return nil, 0, fmt.Errorf("unexpected res[1][%d] type: %T", i, expireSlice[i])
		}
		offset, ok := expireSlice[i+1].(int64)
		if !ok {
			return nil, 0, fmt.Errorf("unexpected res[1][%d] type: %T", i+1, expireSlice[i+1])
		}
		itemID, ok := expireSlice[i+2].(int64)
		if !ok {
			return nil, 0, fmt.Errorf("unexpected res[1][%d] type: %T", i+2, expireSlice[i+2])
		}
		expirations[i/3] = Expiration{
			Worker: worker,
			Offset: offset,
			ItemID: itemID,
		}
	}
	var waitTime time.Duration
	if nextExpiry == 0 {
		waitTime = -1
	} else if nextExpiry <= nowUnix {
		waitTime = 0
	} else {
		waitTime = time.Duration(nextExpiry - nowUnix) * time.Second
	}
	return expirations, waitTime, nil
}

// ackScript removes acknowledged task assignments for a worker.
// Keys:
// 1. Sorted Set in-flight tasks
// 2. Sorted Set expiration queue
// Arguments:
// 1. Worker ID
// 2. List of Kafka message offsets to remove
// Returns: Number of removed assignments
const ackScript = `
-- Keys
local key_inflight = KEYS[1]
local key_expiry = KEYS[2]

-- Arguments
local worker = ARGV[1]
local offsets = ARGV[2]

local count = 0
for offset in offsets do
  local items_pending = redis.call("ZRANGEBYLEX", key_inflight, 
    "[" .. struct.pack("lll", worker, offset, 0),
    "(" .. struct.pack("lll", worker, offset+1, 0))
  count = count + redis.call("ZREM", key_expiry, items_pending)
end
return count
`

// EvalAck acknowledges a bunch of in-flight Kafka messages by their offsets for a worker.
func (r *RedisClient) EvalAck(ctx context.Context, worker int64, offsets []int64) (uint, error) {
	keys := []string{r.PartitionKeys.Inflight, r.PartitionKeys.Expirations}
	count, err := r.ack.Run(ctx, r.Redis, keys, worker, offsets).Int64()
	return uint(count), err
}
