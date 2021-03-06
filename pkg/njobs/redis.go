package njobs

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/go-redis/redis/v8"
	"go.od2.network/hive-api"
	"go.od2.network/hive/pkg/topology"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// FIXME Expired sessions / assignments should become invisible

// PartitionKeys specifies the Redis keys for partition-scoped information.
type PartitionKeys struct {
	// Session
	SessionSerial  string // Hash Map: worker => session serial number
	SessionCount   string // Hash Map: worker => number of active sessions
	SessionExpires string // Sorted Set: {worker, session} by exp_time

	// Flow control
	WorkerQuota  string // Hash Map: Remaining message limit per worker
	SessionQuota string // Hash Map: Remaining message limit per session

	// NAssign algorithm
	Offset        string // Int64: Kafka partition offset (global worker offset)
	TaskAssigns   string // Hash Map: message => no. of assignments
	ActiveWorkers string // Sorted Set: Active worker offsets
	WorkerOffsets string // Hash Map: worker => offset

	// Delivery
	WorkerQueuePrefix string // Prefix for Stream: Worker queue {worker, offset, item}
	Results           string // Stream: Worker results {worker, offset, item, ok}
}

// WorkerQueue returns the specific worker queue based on WorkerQueuePrefix.
func (p *PartitionKeys) WorkerQueue(worker int64) string {
	var workerBytes [8]byte
	binary.BigEndian.PutUint64(workerBytes[:], uint64(worker))
	return p.WorkerQueuePrefix + string(workerBytes[:])
}

// NewPartitionKeys returns the default PartitionKeys for a given Kafka partition.
func NewPartitionKeys(topic string, partition int32) PartitionKeys {
	return PartitionKeys{
		// Session
		SessionSerial:  partitionKey(topic, partition, 0x10),
		SessionCount:   partitionKey(topic, partition, 0x11),
		SessionExpires: partitionKey(topic, partition, 0x12),
		// Flow control
		WorkerQuota:  partitionKey(topic, partition, 0x20),
		SessionQuota: partitionKey(topic, partition, 0x21),
		// N-Assign
		Offset:        partitionKey(topic, partition, 0x30),
		TaskAssigns:   partitionKey(topic, partition, 0x31),
		ActiveWorkers: partitionKey(topic, partition, 0x32),
		WorkerOffsets: partitionKey(topic, partition, 0x33),
		// Delivery
		WorkerQueuePrefix: partitionKey(topic, partition, 0x40),
		Results:           partitionKey(topic, partition, 0x41),
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
	*topology.Collection
	PartitionKeys PartitionKeys
	// Redis scripts
	scripts *scripts
}

// NewRedisClient creates an NJobs Redis client for a given shard.
//
// Useful for dependency injection.
func NewRedisClient(
	rd *redis.Client,
	shard *topology.Shard,
	opts *topology.Collection,
) (*RedisClient, error) {
	// Connect to Redis njobs.
	return &RedisClient{
		Redis:         rd,
		PartitionKeys: NewPartitionKeys(shard.Collection, shard.Partition),
		scripts:       getScripts(),
		Collection:    opts,
	}, nil
}

type scripts struct {
	// Task control
	assignTasks *redis.Script
	expireTasks *redis.Script
	ack         *redis.Script
	// Session control
	startSession      *redis.Script
	stopSession       *redis.Script
	addSessionQuota   *redis.Script
	resetSessionQuota *redis.Script
	commitRead        *redis.Script
}

var scriptsOnce sync.Once
var scriptsSingleton scripts

func getScripts() *scripts {
	scriptsOnce.Do(func() {
		scriptsSingleton = scripts{
			// Task control
			assignTasks: redis.NewScript(assignTasksScript),
			expireTasks: redis.NewScript(expireTasksScript),
			ack:         redis.NewScript(ackScript),
			// Session control
			startSession:      redis.NewScript(startSessionScript),
			stopSession:       redis.NewScript(stopSessionScript),
			addSessionQuota:   redis.NewScript(addSessionQuotaScript),
			resetSessionQuota: redis.NewScript(resetSessionQuotaScript),
			commitRead:        redis.NewScript(commitReadScript),
		}
	})
	return &scriptsSingleton
}

// startSessionScript creates a new worker session.
// If the started session is the first, the worker delivery pipeline also gets spun up.
// Keys:
// 1. Hash Map sessions serial
// 2. Hash Map sessions counter
// 3. Sorted Set session expires
// Arguments:
// 1. Worker ID
// 2. Session expire time
// Returns: Session ID
// language=Lua
const startSessionScript = `
-- Keys
local key_session_serial = KEYS[1]
local key_session_count = KEYS[2]
local key_session_expires = KEYS[3]

-- Arguments
local worker = ARGV[1]
local expire_time = ARGV[2]
local stream_prefix = ARGV[3]

-- Create session.
local worker_key = struct.pack(">l", worker)
local session_id = redis.call("HINCRBY", key_session_serial, worker_key, 1)
local session_count = redis.call("HINCRBY", key_session_count, worker_key, 1)
-- redis.log(redis.LOG_VERBOSE,
--  string.format("njobs:add_session(%d) => id=%d total=%d", worker, session_id, session_count))
if session_count == 1 then
  -- First session has to allocate worker queue.
  redis.log(redis.LOG_VERBOSE, string.format("njobs:add_worker(%d)", worker))
  local worker_stream_key = stream_prefix .. struct.pack(">l", worker)
  redis.call("XGROUP", "CREATE", worker_stream_key, "main", 0, "MKSTREAM")
end
redis.call("ZADD", key_session_expires, expire_time, struct.pack(">ll", worker, session_id))
return session_id
`

// EvalStartSession moves a worker to the active set, giving it tasks.
func (r *RedisClient) EvalStartSession(ctx context.Context, worker int64, unixNow int64) (sessionID int64, err error) {
	keys := []string{
		r.PartitionKeys.SessionSerial,
		r.PartitionKeys.SessionCount,
		r.PartitionKeys.SessionExpires,
	}
	sessionExp := unixNow + int64(r.Collection.SessionTimeout.Seconds())
	return r.scripts.startSession.Run(ctx, r.Redis, keys, worker, sessionExp, r.PartitionKeys.WorkerQueuePrefix).Int64()
}

// stopSessionScript is a hybrid script that runs with two input modes, removing sessions.
// If no sessions exist anymore, the worker delivery pipeline gets deleted.
//
// Mode 0 stops and removes a specific session.
//
// Mode 1 stops and removes all sessions that have expired (i.e. not refreshed in the timeout interval).
//
// Usage
//
//   Keys:
//   1. Sorted Set active workers
//   2. Sorted Set sessions per worker
//   3. Sorted Set session expires
//   4. Hash Map session quota
//   5. Hash Map worker quota
//   6. Stream results
//   Arguments:
//   1. Mode
//   2. Prefix stream worker
//   Arguments (mode 0):
//   3. Worker ID
//   4. Session ID
//   Arguments (mode 1):
//   3. Current unix epoch
//   4. Max sessions to clean up at once
//   5. Prefix of worker stream keys
//
// language=Lua
const stopSessionScript = `
-- Keys
local key_active_workers = KEYS[1]
local key_session_count = KEYS[2]
local key_session_expires = KEYS[3]
local key_session_quota = KEYS[4]
local key_worker_quota = KEYS[5]
local key_results = KEYS[6]

local mode = tonumber(ARGV[1])
local stream_prefix = ARGV[2]

local function remove_worker (worker)
  redis.log(redis.LOG_VERBOSE, string.format("njobs:remove_worker(%d)", worker))
  -- All sessions are gone, terminate worker and dead-letter.
  local worker_key = struct.pack(">l", worker)
  local key_worker_stream = stream_prefix .. worker_key
  local msgs = redis.call("XRANGE", key_worker_stream, "-", "+")
  for i,msg in ipairs(msgs) do
    local msg_id = msg[1]
    local offset = string.sub(msg_id, 1, string.find(msg_id, "-") - 1)
    redis.call("XADD", key_results, "*",
      "worker", worker,
      "offset", offset,
      "status", 2,
      unpack(msg[2]))
  end
  redis.call("ZREM", key_active_workers, worker_key)
  redis.call("HDEL", key_worker_quota, worker_key)
  redis.call("HDEL", key_session_count, worker_key)
  redis.call("DEL", key_worker_stream)
end

local function remove_session (worker, session)
  local worker_key = struct.pack(">l", worker)
  local session_key = struct.pack(">ll", worker, session)
  local deleted = redis.call("ZREM", key_session_expires, session_key)
  local num_sessions = redis.call("HINCRBY", key_session_count, worker_key, -1)
  -- redis.log(redis.LOG_VERBOSE,
  --  string.format("njobs:remove_session(%d, %d) => total=%d", worker, session, num_sessions))
  redis.call("HDEL", key_session_quota, session_key)
  -- Ideally we also dead-letter the stream pending entries assigned to this session,
  -- but the remaining task expire workers will catch those anyways.
  if num_sessions <= 0 then
    remove_worker (worker)
  end
  return deleted
end

if mode == 0 then
  -- Remove session entry.
  local worker = tonumber(ARGV[3])
  local session = tonumber(ARGV[4])
  return remove_session(worker, session)
elseif mode == 1 then
  local time = tonumber(ARGV[3])
  local limit = tonumber(ARGV[4])
  -- Pop expired items
  local expired = redis.call("ZRANGEBYSCORE", key_session_expires, "-inf", time-1, "LIMIT", 0, limit)
  if #expired > 0 then
    redis.call("ZREM", key_session_expires, unpack(expired))
  end
  for i,session_key in ipairs(expired) do
    local worker, session = struct.unpack(">ll", session_key)
    remove_session(worker, session)
  end
  -- Get time of next expiry
  local next_expiry_entry = redis.call("ZRANGE", key_session_expires, 0, 0, "WITHSCORES")
  local next_expiry = 0
  if #next_expiry_entry >= 2 then
    next_expiry = next_expiry_entry[2]
  end
  return next_expiry
else
  error("remove_session: unknown mode " .. tostring(mode))
end
`

// EvalStopSession moves a worker to the stopped set, removing all tasks.
func (r *RedisClient) EvalStopSession(ctx context.Context, worker int64, session int64) error {
	keys := []string{
		r.PartitionKeys.ActiveWorkers,
		r.PartitionKeys.SessionCount,
		r.PartitionKeys.SessionExpires,
		r.PartitionKeys.SessionQuota,
		r.PartitionKeys.WorkerQuota,
		r.PartitionKeys.Results,
	}
	err := r.scripts.stopSession.Run(ctx, r.Redis, keys,
		0, r.PartitionKeys.WorkerQueuePrefix,
		worker, session).Err()
	if errors.Is(err, redis.Nil) {
		return nil
	}
	return err
}

// ErrSessionNotFound is thrown when trying to access a non-existent session.
// This usually happens when trying to refresh an expired session.
var ErrSessionNotFound = status.Error(codes.NotFound, "session not found")

// evalSessionExpire pops expired sessions and returns the time to wait until next expiry.
// It processes at most "limit" sessions at once.
// The "time to wait" is set to -1 when there are no more items.
func (r *RedisClient) evalSessionExpire(ctx context.Context, now int64, limit int64) (sleep time.Duration, err error) {
	keys := []string{
		r.PartitionKeys.ActiveWorkers,
		r.PartitionKeys.SessionCount,
		r.PartitionKeys.SessionExpires,
		r.PartitionKeys.SessionQuota,
		r.PartitionKeys.WorkerQuota,
		r.PartitionKeys.Results,
	}
	nextExpiry, err := r.scripts.stopSession.Run(ctx, r.Redis, keys,
		1, r.PartitionKeys.WorkerQueuePrefix,
		now, limit).Int64()
	if err != nil {
		return 0, err
	}
	var waitTime time.Duration
	if nextExpiry == 0 {
		waitTime = r.SessionTimeout // if no items, safe to sleep for full TTL
	} else if nextExpiry <= now {
		waitTime = 0
	} else {
		waitTime = time.Duration(nextExpiry-now) * time.Second
	}
	return waitTime, nil
}

// language=Lua
const addSessionQuotaScript = `
-- Keys
local key_worker_quota = KEYS[1]
local key_session_quota = KEYS[2]
local key_session_expire = KEYS[3]
local key_worker_offsets = KEYS[4]
local key_active_workers = KEYS[5]
-- Arguments
local worker = ARGV[1]
local session = ARGV[2]
local quota = ARGV[3]

-- TODO Implement worker quota cap.
local worker_key = struct.pack(">l", worker)
local session_key = struct.pack(">ll", worker, session)
if redis.call("ZSCORE", key_session_expire, struct.pack(">ll", worker, session)) == nil then
  return nil
end
redis.call("HINCRBY", key_worker_quota, worker_key, quota)
local added = redis.call("HINCRBY", key_session_quota, session_key, quota)
-- Make worker active.
local offset_bin = redis.call("HGET", key_worker_offsets, worker_key)
local offset
if offset_bin then
  offset = struct.unpack(">l", offset_bin)
else
  offset = -1
  redis.call("HSET", key_worker_offsets, worker_key, struct.pack(">l", -1))
  redis.log(redis.LOG_VERBOSE, string.format("njobs: worker_first_task worker=%d", worker))
end
redis.call("ZADD", key_active_workers, offset, worker_key)
-- redis.log(redis.LOG_DEBUG, string.format("njobs:add_session_quota(%d, %d, %d)", worker, session, quota))
return added
`

// AddSessionQuota adds more quota to a session.
func (r *RedisClient) AddSessionQuota(
	ctx context.Context,
	worker int64, session int64,
	n int64,
) (newQuota int64, err error) {
	keys := []string{
		r.PartitionKeys.WorkerQuota,
		r.PartitionKeys.SessionQuota,
		r.PartitionKeys.SessionExpires,
		r.PartitionKeys.WorkerOffsets,
		r.PartitionKeys.ActiveWorkers,
	}
	res, err := r.scripts.addSessionQuota.Run(ctx, r.Redis, keys, worker, session, n).Int64()
	if errors.Is(err, redis.Nil) {
		return 0, ErrSessionNotFound
	} else if err != nil {
		return 0, fmt.Errorf("failed to run addSessionQuota: %w", err)
	}
	return res, nil
}

// language=Lua
const resetSessionQuotaScript = `
-- Keys
local key_worker_quota = KEYS[1]
local key_session_quota = KEYS[2]
-- Arguments
local worker = ARGV[1]
local session = ARGV[2]

local worker_key = struct.pack(">l", worker)
local session_key = struct.pack(">ll", worker, session)
local quota_str = redis.call("HGET", key_session_quota, session_key)
if not quota_str then
  return nil
end
local quota = tonumber(quota_str)
redis.call("HINCRBY", key_worker_quota, worker_key, -quota)
redis.call("HSET", key_session_quota, session_key, 0)
return quota
`

// ResetSessionQuota resets the quota of a session to zero.
func (r *RedisClient) ResetSessionQuota(
	ctx context.Context,
	worker int64, session int64,
) (removedQuota int64, err error) {
	keys := []string{
		r.PartitionKeys.WorkerQuota,
		r.PartitionKeys.SessionQuota,
	}
	res, err := r.scripts.resetSessionQuota.Run(ctx, r.Redis, keys, worker, session).Int64()
	if errors.Is(err, redis.Nil) {
		return 0, ErrSessionNotFound
	}
	return res, nil
}

// assignTasksScript takes in a batch of tasks and distributes it across workers.
// Keys:
// 1. String topic offset
// 2. Hash Map message => tries
// 3. Sorted Set active worker offsets
// 4. Hash Map worker offsets
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
//
// language=Lua
const assignTasksScript = `
-- Keys
local key_progress = KEYS[1]
local key_message_tries = KEYS[2]
local key_active_workers = KEYS[3]
local key_worker_offsets = KEYS[4]
local key_worker_quota = KEYS[5]
-- Arguments
local replicas = ARGV[1]
local exp_time = ARGV[2]
local stream_prefix = ARGV[3]

-- Loop through messages
local progress = tonumber(redis.call("GET", key_progress) or 0)
local assigns = 0

local function run ()
  for i=4,#ARGV,2 do
    local offset = tonumber(ARGV[i])
    local item = ARGV[i+1]
    if offset < progress then
      -- Kafka consumer is behind Redis state.
      return "ERR_SEEK"
    end
    -- Assign each message N times
	-- TODO Use binary key?
    local tries = tonumber(redis.call("HGET", key_message_tries, offset))
	if not tries then
	  tries = 0
	end
    for j=tries,replicas-1,1 do
      -- Pop worker with lowest progress.
      local worker_p = redis.call("ZPOPMIN", key_active_workers)
      if #worker_p == 0 then
        return "ERR_NO_WORKERS"
      end
      local worker_key = worker_p[1]
      local worker_progress = tonumber(worker_p[2])
      -- Check if this worker is already ahead of message.
      -- This happens either if Kafka rewinds or there are less workers than task assignment replicas.
      if worker_progress >= offset then
        redis.call("ZADD", key_active_workers, worker_progress, worker_key)
        return "ERR_NO_WORKERS"
      end
      -- Update worker progress.
      redis.call("ZADD", key_active_workers, offset, worker_key)
      redis.call("HSET", key_worker_offsets, worker_key, struct.pack(">l", offset))
      -- Push task to worker queue.
      local key_worker_stream = stream_prefix .. worker_key
      redis.call("XADD", key_worker_stream, tostring(offset) .. "-1",
        "exp_time", exp_time, "item", item)
      assigns = assigns + 1
      -- Consume worker quota, and pause worker if out of quota.
      local worker_quota = redis.call("HINCRBY", key_worker_quota, worker_key, -1)
      if worker_quota <= 0 then
        redis.call("ZREM", key_active_workers, worker_key)
      end
	  redis.call("HSET", key_message_tries, offset, tries+1)
    end
    -- Move forward progress.
    progress = offset
    redis.call("HDEL", key_message_tries, offset)
    redis.call("SET", key_progress, offset)
  end
  return "OK"
end

local err = run()
redis.log(redis.LOG_VERBOSE, string.format("njobs:assign_tasks() => progress=%d assigns=%d", progress, assigns))
return {progress, assigns, err}
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
func (r *RedisClient) evalAssignTasks(ctx context.Context, batch []*sarama.ConsumerMessage) (newOffset int64, count int64, err error) {
	expireAt := time.Now().Add(r.TaskTimeout).Unix()
	keys := []string{
		r.PartitionKeys.Offset,
		r.PartitionKeys.TaskAssigns,
		r.PartitionKeys.ActiveWorkers,
		r.PartitionKeys.WorkerOffsets,
		r.PartitionKeys.WorkerQuota,
	}
	// Assemble the arguments.
	argv := make([]interface{}, 3+len(batch)*2)
	argv[0] = int64(r.TaskAssignments)
	argv[1] = expireAt
	argv[2] = r.PartitionKeys.WorkerQueuePrefix
	offsetsMap := make(map[int64]*sarama.ConsumerMessage)
	lastOffset := int64(-1)
	for i, msg := range batch {
		// Offset has to be strictly monotonically increasing!
		// Violating this is going to stall the pipeline.
		if lastOffset >= msg.Offset {
			err = fmt.Errorf("offsets in batch at %d are not strictly monotonically increasing: "+
				"got %d, previous %d", i, msg.Offset, lastOffset)
			return
		}
		lastOffset = msg.Offset
		argv[3+i*2] = msg.Offset
		argv[3+(i*2)+1] = msg.Key
		offsetsMap[msg.Offset] = msg
	}
	cmd := r.scripts.assignTasks.Run(ctx, r.Redis, keys, argv...)
	err = cmd.Err()
	if err != nil {
		return
	}
	res := cmd.Val()
	resSlice, ok := res.([]interface{})
	if !ok {
		err = fmt.Errorf("unexpected res: %#v", res)
		return
	}
	if len(resSlice) != 3 {
		err = fmt.Errorf("unexpected res len: %d", len(resSlice))
		return
	}
	newOffset, ok = resSlice[0].(int64)
	if !ok {
		err = fmt.Errorf("unexpected res[0]: %#v", resSlice[0])
		return
	}
	count, ok = resSlice[1].(int64)
	if !ok {
		err = fmt.Errorf("unexpected res[1]: %#v", resSlice[1])
		return
	}
	retCode, ok := resSlice[2].(string)
	if !ok {
		err = fmt.Errorf("unexpected res[1]: %#v", resSlice[2])
		return
	}
	var retErr error
	switch retCode {
	case "OK":
		retErr = nil
	case "ERR_SEEK":
		retErr = ErrSeek
	case "ERR_NO_WORKERS":
		retErr = ErrNoWorkers
	default:
		retErr = fmt.Errorf("unknown error code: %s", retCode)
	}
	return newOffset, count, retErr
}

// expireTasksScript removes expired in-flight assignments for a worker stream.
// Keys:
// 1. Stream worker
// 2. Stream results
// Arguments:
// 1. Current unix time
// 2. Worker ID
// Returns next expiration time
//
// language=Lua
const expireTasksScript = `
-- Keys
local key_worker_stream = KEYS[1]
local key_results = KEYS[2]
-- Arguments
local unix_time = tonumber(ARGV[1])
local worker = tonumber(ARGV[2])
local batch = tonumber(ARGV[3])

local function parse_kvps (kvps)
  local res = {}
  for i=1,#kvps,2 do
    res[kvps[i]] = kvps[i+1]
  end
  return res
end

-- Loop through all expired items on the stream 
while true do
  local msgs = redis.call("XRANGE", key_worker_stream, "-", "+", "COUNT", batch)
  if #msgs <= 0 then
    return 0
  end
  for i,msg in ipairs(msgs) do
    local msg_id = msg[1]
    local offset = string.sub(msg_id, 1, string.find(msg_id, "-") - 1)
    local kvps = parse_kvps(msg[2])
    local exp_time = tonumber(kvps["exp_time"])
    if exp_time < unix_time then
      redis.call("XADD", key_results, "*",
        "worker", worker,
        "offset", offset,
        "status", 2,
        unpack(msg[2]))
      redis.call("XDEL", key_worker_stream, msg[1])
    else
      return exp_time
    end
  end
end
`

// evalExpire pops expired items form a worker stream.
// It returns the timestamp when the next expiry happens, or 0 if unknown.
func (r *RedisClient) evalExpire(ctx context.Context, worker int64, batch uint) (int64, error) {
	keys := []string{
		r.PartitionKeys.WorkerQueue(worker),
		r.PartitionKeys.Results,
	}
	nowUnix := time.Now().Unix()
	return r.scripts.expireTasks.Run(ctx, r.Redis, keys, nowUnix, worker, int64(batch)).Int64()
}

// ackScript removes acknowledged task assignments for a worker stream.
// Keys:
// 1. Stream worker
// 2. Stream results
// Arguments:
// 1. Worker ID
// 2*N+0: Kafka offset to ack
// 2*N+1: Status
// Returns: Number of removed assignments
//
// language=Lua
const ackScript = `
-- Keys
local key_worker_stream = KEYS[1]
local key_results = KEYS[2]
-- Arguments
local worker = ARGV[1]

local count = 0
for i=2,#ARGV,2 do
  local offset = tonumber(ARGV[i])
  local status = tonumber(ARGV[i+1])
  -- Pop details from task queue.
  local msg_id = ARGV[i] .. "-1"
  local xrange = redis.call("XRANGE", key_worker_stream, msg_id, msg_id, "COUNT", 1)
  if #xrange >= 1 then
    -- Remove key from stream.
    local deleted = redis.call("XDEL", key_worker_stream, msg_id)
    count = count + deleted
    -- Republish on results queue.
    redis.call("XADD", key_results, "*",
      "worker", worker,
      "offset", offset,
      "status", status,
      unpack(xrange[1][2]))
  end
end
return count
`

// EvalAck acknowledges a bunch of in-flight Kafka messages by their offsets for a worker.
func (r *RedisClient) EvalAck(ctx context.Context, worker int64, results []*hive.AssignmentReport) (uint, error) {
	keys := []string{
		r.PartitionKeys.WorkerQueue(worker),
		r.PartitionKeys.Results,
	}
	argv := make([]interface{}, 1+len(results)*2)
	argv[0] = worker
	for i, a := range results {
		argv[1+(i*2)] = a.KafkaPointer.Offset
		argv[1+(i*2)+1] = int64(a.Status.Number())
	}
	count, err := r.scripts.ack.Run(ctx, r.Redis, keys, argv...).Int64()
	return uint(count), err
}

// language=Lua
const commitReadScript = `
-- Keys
local key_session_expires = KEYS[1]
local key_session_quota = KEYS[2]
-- Arguments
local worker = ARGV[1]
local session = ARGV[2]
local num_msgs = ARGV[3]
local exp_time = ARGV[4]

local session_key = struct.pack(">ll", worker, session)
redis.call("ZADD", key_session_expires, exp_time, session_key)
local new_msgs = redis.call("HINCRBY", key_session_quota, session_key, -num_msgs)
if new_msgs <= 0 then
  redis.call("HDEL", key_session_quota, session_key)
end
`

// EvalCommitRead commits the results of a Redis worker session stream read.
// This should always run after XREADGROUP for a client.
func (r *RedisClient) EvalCommitRead(ctx context.Context, worker int64, session int64, numMsgs int64, expTime int64) error {
	keys := []string{
		r.PartitionKeys.SessionExpires,
		r.PartitionKeys.SessionQuota,
	}
	err := r.scripts.commitRead.Run(ctx, r.Redis, keys, worker, session, numMsgs, expTime).Err()
	if errors.Is(err, redis.Nil) {
		return nil
	}
	return err
}

// GetOffset fetches the current Kafka offset of the assigner.
// It returns redis.Nil when no offset is known.
func (r *RedisClient) GetOffset(ctx context.Context) (int64, error) {
	return r.Redis.Get(ctx, r.PartitionKeys.Offset).Int64()
}

// Session interfaces with a worker session.
type Session struct {
	*RedisClient
	Worker         int64
	Session        int64
	nextExpire     int64
	KafkaPartition int32
}

// Run starts a blocking loop that sends all session messages to a Go channel.
// While it's running the session is kept alive by refreshing it in the background.
// This method does not close the channel after returning.
func (s *Session) Run(ctx context.Context, assignmentsC chan<- []*hive.Assignment) error {
	for ctx.Err() == nil {
		assignments, err := s.step(ctx)
		if err != nil {
			return err
		}
		if len(assignments) == 0 {
			continue
		}
		select {
		case <-ctx.Done():
			return nil
		case assignmentsC <- assignments:
			break // continue
		}
	}
	return ctx.Err()
}

func (s *Session) step(ctx context.Context) ([]*hive.Assignment, error) {
	// Blocking read message batch from Redis group.
	streams, readGroupErr := s.Redis.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    "main",
		Consumer: strconv.FormatInt(s.Session, 10),
		Streams:  []string{s.PartitionKeys.WorkerQueue(s.Worker), ">"},
		Count:    int64(s.Collection.DeliverBatch),
		Block:    250 * time.Millisecond,
		NoAck:    false,
	}).Result()
	var redisReadGroupErr redis.Error
	switch {
	case errors.As(readGroupErr, &redisReadGroupErr):
		if strings.HasPrefix(redisReadGroupErr.Error(), "NOGROUP ") {
			return nil, nil // group does not yet exist, thus no assignments
		}
		fallthrough
	case errors.Is(readGroupErr, redis.Nil):
		return nil, nil // no items available
	case readGroupErr != nil:
		return nil, fmt.Errorf("failed to read from group: %w", readGroupErr)
	}
	// Parse message batch.
	if len(streams) == 0 {
		return nil, nil
	} else if len(streams) != 1 {
		return nil, fmt.Errorf("read from unexpected number of streams: %d", len(streams))
	}
	assignments := make([]*hive.Assignment, len(streams[0].Messages))
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
		} else if itemID == "" {
			return nil, fmt.Errorf("empty item ID for offset %d", offset)
		}
		assignments[i] = &hive.Assignment{
			Locator: &hive.ItemLocator{
				Collection: s.Collection.Name,
				Id:         itemID,
			},
			KafkaPointer: &hive.KafkaPointer{
				Partition: s.KafkaPartition,
				Offset:    offset,
			},
		}
	}
	// Commit read.
	unixTime := time.Now().Unix()
	if len(assignments) > 0 {
		sessionExpTime := unixTime + int64(s.Collection.SessionTimeout.Seconds())
		if err := s.EvalCommitRead(ctx, s.Worker, s.Session, int64(len(assignments)), sessionExpTime); err != nil {
			return nil, fmt.Errorf("failed to commit read: %w", err)
		}
	}
	// Check for expirations.
	if s.nextExpire <= unixTime {
		var expErr error
		s.nextExpire, expErr = s.evalExpire(ctx, s.Worker, s.Collection.TaskExpireBatch)
		if expErr != nil {
			return nil, fmt.Errorf("failed to expire tasks: %w", expErr)
		}
		if s.nextExpire == 0 {
			// FIXME When there's nothing in the queue, wait for worker timeout
			s.nextExpire = unixTime + 3
		}
	}
	return assignments, nil
}

func redisWorkerKey(workerID int64) string {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(workerID))
	return string(buf[:])
}

func redisSessionKey(workerID int64, sessionID int64) string {
	var buf [16]byte
	binary.BigEndian.PutUint64(buf[:8], uint64(workerID))
	binary.BigEndian.PutUint64(buf[8:], uint64(sessionID))
	return string(buf[:])
}
