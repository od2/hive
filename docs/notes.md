## High Level

Discovery pipeline:
- In: Ad-hoc task insert API
- In: "task results" Kafka topic
- Filter: redisdedup
- Out: Insert new items to MySQL
- Out: Publish new items to Kafka topic "pipeline"

Worker fanout pipeline:
- In: Kafka topic "pipeline"
- Out: redisqueue

Worker watchdog pipeline:
- In: redisqueue expirations
- Out: ???

Tasks service
- Request: Get tasks
  - Step: redisqueue claims
- Request: Complete tasks
- Request: Reject tasks

## Redisqueue

Insert task with NUM completions
- `LPUSH QUEUE_T msgpack{NUM, 0, task}`

Claim task for worker W
- Worker_Pointer: `HGET QUEUE_P W` or 0
- Global_Pointer: `GET QUEUE_C` or 0
- Pointer: -(Worker_Pointer - Global_Pointer) - 1
- HINCR QUEUE_P

Complete task for worker W

Requeue task for worker W

## Redis Streams => Go

### Expirations

Sender:
- `XADD exp ...` send expiration
- `ZREM` clean up Redis

Receiver init:
- Check if group exists
- `XGROUP CREATE stream main 0 MKSTREAM`

Receiver:
- `XREADGROUP main STREAM stream >` read expirations
- Kafka publish expirations
- `XACK stream main ...` expirations
- `XDEL stream main ...` expirations

### Assignments

Variables
- `s`: Number of streams
- `a`: Number of assignments in-flight

Assigner Init (Transition `s == 0` => `s > 0`)
- `XGROUP CREATE worker_assign_YY main 0 MKSTREAM`

Assigner Send:
- `XADD worker_assign_YY`

Streamer start:
- `SADD worker_streams_YY stream_ZZ`
- Check if group exists
- `XGROUP CREATE worker_assign main 0 MKSTREAM`

Streamer stop (delayed):
- `SREM worker_YY stream_ZZ`

Streamer Send:
- `XREADGROUP main STREAM worker_YY`
- gRPC async send
- `XACK worker_yy main ...`
- `XDEL worker_yy ...`

On each expire/complete:
- `XDEL worker_assign_YY offset`
- `a` <- `XLEN worker_assign`
- `s` <- `SCARD worker_streams_YY`
- Check if `s == 0 and a == 0`
- `DEL worker_assign_YY`

### Flow Control

Request stream quota:
- `HGET worker_max W`: Max quota
- `HGET worker_pending W`: Pending quota
- new = min(allowed, requested)
- `HINCRBY session_pending (W,S) new`
- `HINCRBY worker_pending W new`

Async read:
- `HGET session_pending (W,S)`
- `XREADGROUP GROUP main S COUNT count STREAMS worker_assignments >`
- Critical section
- gRPC send

Session expire:
- ``
