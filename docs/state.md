# Hive state

An overview over where state is stored in od2/hive.

## MySQL

MySQL is the primary datastore of od2/hive.

Hive is compatible with Vitess for when horizontal scalability (sharding) is required.

### Management

Management tables hold information about the system itself (authentication etc).

*Tables*:
- `auth_tokens`: The auth token identifiers of workers.

### Collections

There is a table for each collection of items.

Given the collection name `yt.videos`, the resulting table name is `yt_videos_items`.

## Kafka

Kafka is the primary store for any kind of event logs and pipelines.

Kafka is horizontally scalable itself, by sharding event logs into partitions.

_Topics_:
* `hive.<namespace>.<collection>.tasks`: Scraping task queue
  * Producer: `discovery.Worker`
  * Consumer: `njobs.Assigner`
* `hive.<namespace>.<collection>.discovered`: Reports of newly discovered items
  * Producer: `njobs.Streamer`
  * Consumer: `discovery.Worker`
* `hive.<namespace>.<collection>.results`: Scraping result reports
  * Producer: `njobs.ResultForwarder`
  * Consumer: `reporter.Worker`

## Redis

We use Redis as data structures servers for secondary indexes, real-time state, and accelerators for hot data.

Hive supports sharding the workload over multiple Redis instances to achieve horizontal scalability.
