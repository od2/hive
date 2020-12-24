package njobs

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/go-redis/redis/v8"
	"go.od2.network/hive/pkg/types"
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
	// NAssign algorithm
	Progress      string // Int64: Kafka partition offset (global worker offset)
	TaskAssigns   string // Hash Map: message => no. of assignments
	ActiveWorkers string // Sorted Set: Active worker offsets
	IdleWorkers   string // Hash Map: worker => {offset, mode}
	// Session tracking
	WorkerQuota    string // Hash Map: Remaining message limit per worker
	SessionQuota   string // Hash Map: Remaining message limit per session
	SessionSerial  string // Int64: Session serial number
	SessionCounter string // Int64: Number of active sessions per worker
	SessionExpires string // Sorted Set: {worker, session} by exp_time
	// Event streaming
	TaskExpires string // Sorted Set: {worker, offset} by exp_time
	WorkerQueue string // Prefix for Stream: Worker queue {worker, offset, item}
	Results     string // Stream: Worker results {worker, offset, item, ok}
}

// NewPartitionKeys returns the default PartitionKeys for a given Kafka partition.
func NewPartitionKeys(topic string, partition int32) PartitionKeys {
	return PartitionKeys{
		// NAssign algorithm
		Progress:      partitionKey(topic, partition, 0x10),
		TaskAssigns:   partitionKey(topic, partition, 0x11),
		ActiveWorkers: partitionKey(topic, partition, 0x12),
		IdleWorkers:   partitionKey(topic, partition, 0x13),
		// Session tracking
		WorkerQuota:    partitionKey(topic, partition, 0x20),
		SessionQuota:   partitionKey(topic, partition, 0x21),
		SessionSerial:  partitionKey(topic, partition, 0x22),
		SessionCounter: partitionKey(topic, partition, 0x23),
		SessionExpires: partitionKey(topic, partition, 0x24),
		// Event streaming
		TaskExpires: partitionKey(topic, partition, 0x30),
		WorkerQueue: partitionKey(topic, partition, 0x31),
		Results:     partitionKey(topic, partition, 0x32),
	}
}

func partitionKey(topic string, partition int32, field int8) string {
	var builder strings.Builder
	builder.WriteString("njobs_v0\x01")
	builder.WriteString(topic)
	builder.WriteByte(0x00)
	_ = binary.Write(&builder, binary.BigEndian, partition)
	builder.WriteByte(uint8(field))
	return builder.String()
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
	// Task control
	assignTasks *redis.Script
	expireTasks *redis.Script
	ack         *redis.Script
	// Session control
	startSession    *redis.Script
	stopSession     *redis.Script
	addSessionQuota *redis.Script
}

// LoadScripts hashes the Lua server-side scripts and pre-loads them into Redis.
func LoadScripts(ctx context.Context, r *redis.Client) (*Scripts, error) {
	s := new(Scripts)
	// Task control
	s.assignTasks = redis.NewScript(assignTasksScript)
	if err := s.assignTasks.Load(ctx, r).Err(); err != nil {
		return nil, err
	}
	s.expireTasks = redis.NewScript(expireTasksScript)
	if err := s.expireTasks.Load(ctx, r).Err(); err != nil {
		return nil, err
	}
	s.ack = redis.NewScript(ackScript)
	if err := s.ack.Load(ctx, r).Err(); err != nil {
		return nil, err
	}
	// Session control
	s.startSession = redis.NewScript(startSessionScript)
	if err := s.startSession.Load(ctx, r).Err(); err != nil {
		return nil, err
	}
	s.stopSession = redis.NewScript(stopSessionScript)
	if err := s.stopSession.Load(ctx, r).Err(); err != nil {
		return nil, err
	}
	s.addSessionQuota = redis.NewScript(addSessionQuotaScript)
	if err := s.addSessionQuota.Load(ctx, r).Err(); err != nil {
		return nil, err
	}
	return s, nil
}

// startSessionScript sets the state of a worker to active.
// Keys:
// 1. Hash Map inactive workers
// 2. Sorted Set active worker offsets
// 3. Hash Map sessions serial
// 4. Hash Map sessions counter
// 5. Sorted Set session expires
// Arguments:
// 1. Worker ID
// 2. Session expire time
// Returns: Session ID
const startSessionScript = `
-- Keys
local key_idle_workers = KEYS[1]
local key_active_workers = KEYS[2]
local key_session_serial = KEYS[3]
local key_session_counter = KEYS[4]
local key_session_expires = KEYS[5]

-- Arguments
local worker = ARGV[1]
local expire_time = ARGV[2]

-- Create session.
local session_id = redis.call("HINCRBY", key_session_serial, 1)
redis.call("HINCRBY", key_session_counter, 1)
redis.call("ZADD", key_session_expires, struct.pack("ll", worker, session_id))
-- Unpause worker if exists.
if redis.call("ZSCORE", key_active_workers, worker) ~= nil then
  return
end
local inactive_worker = redis.call("HGET", key_idle_workers, worker)
local offset = 0
local stopped = 1
if not inactive_worker then
  offset, stopped = struct.unpack("lB", inactive_worker)
end
if stopped == 1 then
  redis.call("HDEL", key_idle_workers, ARGV[1])
  redis.call("ZADD", key_active_workers, offset, ARGV[1])
end
return session_id
`

// EvalStartSession moves a worker to the active set, giving it tasks.
func (r *RedisClient) EvalStartSession(ctx context.Context, worker int64) (sessionID int64, err error) {
	keys := []string{
		r.PartitionKeys.IdleWorkers,
		r.PartitionKeys.ActiveWorkers,
	}
	return r.startSession.Run(ctx, r.Redis, keys, worker).Int64()
}

const stopSessionScript = `
-- Keys
local key_idle_workers = KEYS[1]
local key_active_workers = KEYS[2]
local key_session_counter = KEYS[3]
local key_session_expires = KEYS[4]
local key_inflight = KEYS[5]
local key_expiry = KEYS[6]

-- Arguments
local worker = ARGV[1]
local stream_prefix = ARGV[2]

-- Delete session.
local num_sessions = redis.call("HINCRBY", key_session_counter, -1)
if num_sessions > 0 then
  return
end
-- All sessions are gone.
local offset = redis.call("ZSCORE", key_active_workers, worker)
redis.call("ZREM", key_session_expires, struct.pack("ll", worker, session_id))
redis.call("HSET", key_idle_workers, struct.unpack("lB", offset, true))
local key_stream = stream_prefix .. tostring(worker)
redis.call("DEL", key_stream)
-- Get all in-flight messages.
local inflight_start = "[" .. struct.pack("ll", worker, 0)
local inflight_stop  = "(" .. struct.pack("ll", worker+1, 0)
local dropped = redis.call("ZRANGEBYLEX", key_inflight, inflight_start, inflight_stop)
redis.call("ZREMRANGEBYLEX", key_inflight, inflight_start, inflight_stop)
redis.call("ZREM", key_expiry, dropped)
`

// EvalStopSession moves a worker to the stopped set, removing all tasks.
func (r *RedisClient) EvalStopSession(ctx context.Context, worker int64) error {
	keys := []string{
		r.PartitionKeys.IdleWorkers,
		r.PartitionKeys.ActiveWorkers,
	}
	return r.stopSession.Run(ctx, r.Redis, keys, worker).Err()
}

// ErrSessionNotFound is thrown when trying to access a non-existent session.
// This usually happens when trying to refresh an expired session.
var ErrSessionNotFound = errors.New("session not found")

const addSessionQuotaScript = `
-- Keys
local key_worker_quota = KEYS[1]
local key_session_quota = KEYS[2]
-- Arguments
local worker = ARGV[1]
local session = ARGV[2]
local quota = ARGV[3]

-- TODO Implement worker quota cap.
local worker_key = struct.pack("l", worker)
local session_key = struct.pack("ll", worker, session)
if redis.call("HEXISTS", key_session_quota, session_key) ~= 1 then
  return nil
end
redis.call("HINCRBY", key_worker_quota, worker_key, quota)
return redis.call("HINCRBY", key_session_quota, session_key, quota)
`

// AddSessionQuota adds more quota to a session.
func (r *RedisClient) AddSessionQuota(
	ctx context.Context,
	worker int64, session int64,
	n int64,
) (newQuota int64, err error) {
	keys := []string{r.PartitionKeys.WorkerQuota, r.PartitionKeys.SessionQuota}
	res, err := r.addSessionQuota.Run(ctx, r.Redis, keys, worker, session, n).Result()
	if err != nil {
		return 0, fmt.Errorf("failed to run addSessionQuota: %w", err)
	}
	switch a := res.(type) {
	case nil:
		return 0, ErrSessionNotFound
	case int64:
		return a, nil
	default:
		return 0, fmt.Errorf("unexpected res: %T", res)
	}
}

// RefreshSession resets the TTL of a session.
// Session owners need to call this regularly like heart beats.
func (r *RedisClient) RefreshSession(ctx context.Context, worker int64, session int64) error {
	expireUnix := time.Now().Add(r.Options.SessionTimeout).Unix()
	var entry [16]byte
	binary.BigEndian.PutUint64(entry[:8], uint64(worker))
	binary.BigEndian.PutUint64(entry[8:], uint64(session))
	count, err := r.Redis.ZAddXX(ctx, r.PartitionKeys.SessionExpires, &redis.Z{
		Score:  float64(expireUnix),
		Member: entry,
	}).Result()
	if err != nil {
		return err
	}
	if count != 1 {
		return ErrSessionNotFound
	}
	return nil
}

// assignTasksScript takes in a batch of tasks and distributes it across workers.
// Keys:
// 1. String topic offset
// 2. Hash Map message => tries
// 3. Sorted Set active worker offsets
// 4. Hash Map inactive workers
// 5. Sorted Set expiration queue
// Arguments:
// 1. Number of assignments per task
// 2. List of message offsets
// 3. Max pending assignments per worker
// 4. Assignment expiration time
// 5. Prefix for Stream worker assignments
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
local key_expiry = KEYS[5]
local key_worker_quota = KEYS[6]
-- Arguments
local replicas = ARGV[1]
local batch = ARGV[2]
local exp_time = ARGV[3]
local stream_prefix = ARGV[4]

local function key_stream (worker)
  return stream_prefix .. tostring(worker)
end

-- Loop through messages
local progress = redis.call("GET", key_progress) or 0
for i=1,#batch,2 do
  local offset = batch[i]
  local item = batch[i+1]
  if offset <= progress then
    -- Kafka consumer is behind Redis state.
    return {min_offset, "ERR_SEEK"}
  end
  -- Assign each message N times
  local tries = redis.call("HINCRBY", key_message_tries, offset, 1)
  for j=tries,replicas,1 do
    -- Assign task to worker with lowest progress, and update progress.
    local worker_p = redis.call("ZPOPMIN", key_active_workers)
    if #worker_p == 0 then
      return {min_offset, "ERR_NO_WORKERS"}
    end
    local worker = worker_p[1]
    redis.call("ZADD", key_active_workers, offset, worker)
    -- Register task in task set and expiration queue.
    local entry = struct.pack("ll", worker, offset)
    redis.call("ZADD", key_expiry, exp_time, entry)
    redis.call("XADD", key_stream(worker), tostring(offset) .. "-0",
      "exp_time", exp_time, "item", item)
    -- Pause worker if its queue is saturated.
    local worker_quota_key = struct.pack("l", worker)
    local worker_quota = redis.call("HINCRBY", key_worker_quota, worker_quota_key, -1)
    if worker_quota <= 0 then
      redis.call("ZREM", key_active_workers, worker)
      redis.call("HSET", key_idle_workers, workers, struct.pack("lB", offset, false))
      redis.call("HDEL", key_worker_quota, worker_quota_key)
    end
  end
  -- Move forward progress.
  progress = offset
  redis.call("HDEL", key_message_tries, offset)
  redis.call("SET", key_progress, offset)
end
return {offset, "OK"}
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
func (r *RedisClient) evalAssignTasks(ctx context.Context, batch []*sarama.ConsumerMessage) (int64, error) {
	expireAt := time.Now().Add(r.TaskTimeout).Unix()
	keys := []string{
		r.PartitionKeys.Progress,
		r.PartitionKeys.TaskAssigns,
		r.PartitionKeys.ActiveWorkers,
		r.PartitionKeys.IdleWorkers,
		r.PartitionKeys.TaskExpires,
	}
	tasks := make([]interface{}, len(batch))
	offsetsMap := make(map[int64]*sarama.ConsumerMessage)
	for i, msg := range batch {
		tasks[i*2] = msg.Offset
		// Decode Kafka key as big endian int64 item ID.
		if len(msg.Key) != 8 {
			return 0, fmt.Errorf("invalid Kafka key: %x", msg.Key)
		}
		tasks[(i*2)+1] = int64(binary.BigEndian.Uint64(msg.Key))
		offsetsMap[msg.Offset] = msg
	}
	cmd := r.assignTasks.Run(ctx, r.Redis, keys, int64(r.N), tasks, expireAt)
	res, err := cmd.Result()
	if err != nil {
		return 0, err
	}
	resSlice, ok := res.([]interface{})
	if !ok {
		return 0, fmt.Errorf("unexpected res type: %T", res)
	}
	if len(resSlice) != 2 {
		return 0, fmt.Errorf("unexpected res len: %d", len(resSlice))
	}
	newOffset, ok := resSlice[0].(int64)
	if !ok {
		return 0, fmt.Errorf("unexpected res[0] type: %T", resSlice[0])
	}
	status, ok := resSlice[1].(string)
	if !ok {
		return 0, fmt.Errorf("unexpected res[1] type: %T", resSlice[2])
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
	return newOffset, retErr
}

// expireTasksScript removes expired in-flight assignments.
// Keys:
// 1. Sorted Set expiration queue
// 2. Stream results
// Arguments:
// 1. Current unix time
// 2. Max no. of items to expire this invocation
// 3. Prefix for Stream worker assignments
// Returns next expiration time
const expireTasksScript = `
-- Keys
local key_expiry = KEYS[1]
local key_results = KEYS[2]
-- Arguments
local time = ARGV[1]
local limit = ARGV[2]
local stream_prefix = ARGV[3]

local key_stream = stream_prefix .. tostring(worker)
local function find_item (kvps)
  for i=1,#kvps,2 do
    if kvps[i] == "item" then
      return kvps[i+1]
    end
  end
  error("no item field on stream item")
end

-- Pop expired items
local expired = redis.call("ZRANGEBYSCORE", key_expiry, "-inf", time-1, "LIMIT", 0, limit)
redis.call("ZREM", key_inflight, expired)
for expire in expired do
  -- Read key from expire priority queue.
  local worker, offset = struct.unpack("ll", expire)
  -- Pop details from task queue.
  local stream_id = tostring(offset) .. "-0"
  local xrange = redis.call("XRANGE", key_stream, "[" .. stream_id, "(" .. stream_id, "COUNT", 1)
  local item = find_item(xrange[1][2])
  redis.call("XDEL", key_stream, stream_id)
  -- Republish on results queue.
  redis.call("XADD", key_results, "*",
    "worker", worker, "offset", offset, "item", item, "ok", false)
end
-- Get time of next expiry
local next_expiry_entry = redis.call("ZRANGE", key_expire, 0, 0, "WITHSCORES")
local next_expiry = 0
if next_expiry_entry then
  next_expiry = next_expiry_entry[2]
end
return next_expiry
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
func (r *RedisClient) evalExpire(ctx context.Context, limit uint) (time.Duration, error) {
	nowUnix := time.Now().Unix()
	keys := []string{r.PartitionKeys.TaskExpires, r.PartitionKeys.Results}
	res, err := r.expireTasks.Run(ctx, r.Redis, keys,
		nowUnix, int64(limit), r.PartitionKeys.WorkerQueue).Result()
	if err != nil {
		return 0, err
	}
	nextExpiry, ok := res.(int64)
	if !ok {
		return 0, fmt.Errorf("unexpected res type: %T", res)
	}
	var waitTime time.Duration
	if nextExpiry == 0 {
		waitTime = -1
	} else if nextExpiry <= nowUnix {
		waitTime = 0
	} else {
		waitTime = time.Duration(nextExpiry-nowUnix) * time.Second
	}
	return waitTime, nil
}

// ackScript removes acknowledged task assignments for a worker.
// Keys:
// 1. Sorted Set expiration queue
// 2. Stream results
// Arguments:
// 1. Worker ID
// 2. List of Kafka message offsets to remove
// 3. Prefix for Stream worker assignments
// Returns: Number of removed assignments
const ackScript = `
-- Keys
local key_expiry = KEYS[1]
local key_results = KEYS[2]
-- Arguments
local worker = ARGV[1]
local offsets = ARGV[2]
local stream_prefix = ARGV[3]

local key_stream = stream_prefix .. tostring(worker)

-- Helper function to find the "item" key-value pair on a Redis streams item.
local function find_item (kvps)
  for i=1,#kvps,2 do
    if kvps[i] == "item" then
      return kvps[i+1]
    end
  end
  error("no item field on stream item")
end

local count = 0
for offset in offsets do
  -- Pop details from task queue.
  local stream_id = tostring(offset) .. "-0"
  local xrange = redis.call("XRANGE", key_stream, "[" .. stream_id, "(" .. stream_id, "COUNT", 1)
  local item = find_item(xrange[1][2])
  redis.call("XDEL", key_stream, stream_id)
  -- Remove key from expire priority queue.
  redis.call("ZREM", key_expiry, struct.pack("ll", worker, offset))
  count = count + deleted
  -- Republish on results queue.
  redis.call("XADD", key_results, "*",
    "worker", worker, "offset", offset, "item", item, "ok", true)
end
return count
`

// EvalAck acknowledges a bunch of in-flight Kafka messages by their offsets for a worker.
func (r *RedisClient) EvalAck(ctx context.Context, worker int64, offsets []int64) (uint, error) {
	keys := []string{r.PartitionKeys.TaskExpires, r.PartitionKeys.Results}
	count, err := r.ack.Run(ctx, r.Redis, keys, worker, offsets, r.PartitionKeys.WorkerQueue).Int64()
	return uint(count), err
}

// Session interfaces with a worker session.
type Session struct {
	*RedisClient
	Worker  int64
	Session int64
}

// Run starts a blocking loop that sends all session messages to a Go channel.
// While it's running the session is kept alive by refreshing it in the background.
// This method does not close the channel after returning.
func (s *Session) Run(ctx context.Context, assignmentsC chan<- []*types.Assignment) error {
	for {
		assignments, err := s.step(ctx)
		if err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return nil
		case assignmentsC <- assignments:
			break // continue
		}
	}
}

func (s *Session) step(ctx context.Context) ([]*types.Assignment, error) {
	// Read message batch from Redis group.
	streams, err := s.Redis.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    "",
		Consumer: "",
		Streams:  []string{},
		Count:    4,
		Block:    250 * time.Millisecond,
		NoAck:    false,
	}).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to read from group: %w", err)
	}
	if len(streams) == 0 {
		return nil, nil
	} else if len(streams) != 1 {
		return nil, fmt.Errorf("read from unexpected number of streams: %d", len(streams))
	}
	assignments := make([]*types.Assignment, len(streams[0].Messages))
	for i, msg := range streams[0].Messages {
		idParts := strings.SplitN(msg.ID, "-", 2)
		if len(idParts) != 2 {
			return nil, fmt.Errorf(`unexpected read[%d].id: %s`, i, msg.ID)
		}
		offset, err := strconv.ParseInt(idParts[0], 10, 64)
		if err != nil {
			return nil, fmt.Errorf(`unexpected read[%d].id: %s`, i, msg.ID)
		}
		itemID, ok := msg.Values["item"].(string)
		if !ok {
			return nil, fmt.Errorf(`unexpected read[%d]["item"]: %#v`, i, msg.Values["item"])
		}
		assignments[i] = &types.Assignment{
			Locator: &types.ItemLocator{
				Collection: "", // TODO
				Id:         itemID,
			},
			KafkaPartition: 0, // TODO
			KafkaOffset:    offset,
		}
	}
}
