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
