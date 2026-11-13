# Golang asynchronous task/job queue with Redis, SQS, IronMQ, and in-memory backends

![build workflow](https://github.com/vmihailenco/taskq/actions/workflows/build.yml/badge.svg)
[![PkgGoDev](https://pkg.go.dev/badge/github.com/vmihailenco/taskq/v5)](https://pkg.go.dev/github.com/vmihailenco/taskq/v5)
[![Documentation](https://img.shields.io/badge/bun-documentation-informational)](https://taskq.uptrace.dev/)
[![Chat](https://discordapp.com/api/guilds/752070105847955518/widget.png)](https://discord.gg/rWtp5Aj)

> taskq is brought to you by :star: [**uptrace/uptrace**](https://github.com/uptrace/uptrace).
> Uptrace is an open source and blazingly fast
> [distributed tracing tool](https://get.uptrace.dev/compare/distributed-tracing-tools.html) powered
> by OpenTelemetry and ClickHouse. Give it a star as well!

## Features

- Redis, SQS, IronMQ, and in-memory backends.
- Automatically scaling number of goroutines used to fetch (fetcher) and process messages (worker).
- Global rate limiting.
- Global limit of workers.
- Call once - deduplicating messages with same name.
- Automatic retries with exponential backoffs.
- Automatic pausing when all messages in queue fail.
- Fallback handler for processing failed messages.
- Message batching. It is used in SQS and IronMQ backends to add/delete messages in batches.
- Automatic message compression using snappy / s2.

Resources:

- [**Get started**](https://taskq.uptrace.dev/guide/golang-task-queue.html)
- [Examples](https://github.com/vmihailenco/taskq/tree/v3/example)
- [Discussions](https://github.com/uptrace/bun/discussions)
- [Chat](https://discord.gg/rWtp5Aj)
- [Reference](https://pkg.go.dev/github.com/vmihailenco/taskq/v5)

## Requirements

- Go 1.25 or newer (CI runs against Go 1.25 – 1.27). Go 1.25 is out of upstream support and its
  standard library has security fixes that are only available in newer releases, so build
  production binaries with Go 1.26.11+ or 1.27.x.
- [go-redis v9](https://github.com/redis/go-redis) (`github.com/redis/go-redis/v9`) for the Redis
  backend and for global rate limiting / locking.

### Upgrading from v3 to v5

v5 is the v3 API on top of go-redis v9. Because go-redis types are part of taskq's public API,
this is a breaking change, so the module path changes as well. (There is no v4 release line; v4
was skipped to avoid clashing with the earlier, unrelated v4 beta tags.)

To upgrade:

1. Use Go 1.25 or newer.
2. Change the import paths:

```go
// Before (v3)
import "github.com/vmihailenco/taskq/v3"
import "github.com/go-redis/redis/v8"
import "github.com/go-redis/redis_rate/v9"

// After (v5)
import "github.com/vmihailenco/taskq/v5"
import "github.com/redis/go-redis/v9"
import "github.com/go-redis/redis_rate/v10"
```

3. Pass a go-redis v9 client (`*redis.Client`, `*redis.Ring`, `*redis.ClusterClient`, ...) as
   `QueueOptions.Redis`. If your own code also uses go-redis directly, see the
   [go-redis v9 migration notes](https://redis.uptrace.dev/guide/v9-migration.html).
4. Only if you implement taskq's Redis interfaces yourself (go-redis v9 clients already satisfy
   them):
   - `taskq.Redis` gained `EvalRO` and `EvalShaRO`.
   - `redisq.RedisStreamClient` uses `XTrimMaxLen` instead of `XTrim`, and `ZAdd` takes
     `...redis.Z` instead of `...*redis.Z` (go-redis v9 signatures).

Nothing else in the taskq API changed.

## Getting started

To get started, see [Golang Task Queue](https://taskq.uptrace.dev/) documentation.

**Producer**:

```go
import (
    "github.com/vmihailenco/taskq/v5"
    "github.com/vmihailenco/taskq/v5/redisq"
)

// Create a queue factory.
var QueueFactory = redisq.NewFactory()

// Create a queue.
var MainQueue = QueueFactory.RegisterQueue(&taskq.QueueOptions{
    Name:  "api-worker",
    Redis: Redis, // go-redis client
})

// Register a task.
var CountTask = taskq.RegisterTask(&taskq.TaskOptions{
    Name: "counter",
    Handler: func() error {
        IncrLocalCounter()
        return nil
    },
})

ctx := context.Background()

// And start producing.
for {
	// Call the task without any args.
	err := MainQueue.Add(CountTask.WithArgs(ctx))
	if err != nil {
		panic(err)
	}
	time.Sleep(time.Second)
}
```

**Consumer**:

```go
// Start consuming the queue.
if err := MainQueue.Start(context.Background()); err != nil {
    log.Fatal(err)
}
```

## Benchmarks

Both benchmarks below use the Redis backend with a local, otherwise idle Redis instance, 20,000
tasks per run, and worker concurrency of `8 × NumCPU`. Task payloads are random bytes, so they are
not compressible (similar to images or already-compressed data). Absolute numbers depend on the
machine; the two tables were measured on different machines and should not be compared with each
other.

### v5 vs v3

The same benchmark program compiled against taskq v3 (go-redis v8) and taskq v5 (go-redis v9), run
in three alternating rounds; the table shows the median. 8 concurrent producers per CPU enqueue the
tasks, then the consumer is started and timed until every task has been handled. Apple M4 Pro
(12 cores), Redis 8.10, Go 1.26.

| Payload size | Enqueue (tasks/s)<br>v3 / v5 | Consume (tasks/s)<br>v3 / v5 | Redis memory per task<br>v3 / v5 | Redis commands per task<br>(enqueue + consume) |
| -----------: | ---------------------------: | ---------------------------: | -------------------------------: | ---------------------------------------------: |
|        200 B |              60,500 / 60,400 |              21,100 / 21,100 |                    358 B / 358 B |                                3.1 / 3.1 |
|         4 KB |              54,800 / 59,400 |              20,200 / 20,400 |                  5.1 KB / 5.1 KB |                                3.1 / 3.1 |
|        64 KB |              31,800 / 31,200 |                9,100 / 9,200 |                    82 KB / 82 KB |                                3.1 / 3.1 |

There is no measurable performance difference: throughput differences are within run-to-run noise
(the 4 KB enqueue gap is smaller than the spread between rounds), and Redis memory and the number
of Redis commands per task are identical. The only cost of the upgrade is binary size: a minimal
program using the Redis backend grows from 6.2 MB to 7.7 MB (linux/amd64, `-ldflags="-s -w"`),
mostly because go-redis v9 is larger than v8.

### taskq vs asynq

A comparison of taskq (measured with v3, which performs the same as v5, see above) with
[asynq](https://github.com/hibiken/asynq); results are the average of two runs.

| Payload size | Enqueue (tasks/s)<br>taskq / asynq | Consume (tasks/s)<br>taskq / asynq | Redis memory per task<br>taskq / asynq |
| -----------: | ---------------------------------: | ---------------------------------: | -------------------------------------: |
|        200 B |                    99,600 / 70,200 |                    36,400 / 11,800 |                            373 B / 801 B |
|         4 KB |                    69,800 / 50,100 |                    29,700 / 10,700 |                          5.2 KB / 5.6 KB |
|        64 KB |                     21,800 / 8,000 |                     11,400 / 4,500 |                            82 KB / 82 KB |

Observations:

- asynq consumes tasks at roughly 1/3 of taskq's rate. asynq records per-task state (active, retry,
  archived, ...), which costs extra Redis round trips per task. It still processes around 10,000
  small tasks per second.
- For small tasks asynq uses about twice as much Redis memory (~800 B vs ~370 B per task).
- taskq compresses message payloads larger than 512 B; asynq does not. With compressible payloads
  (JSON, text, ...) the memory gap is larger than shown above, where payloads are random.
- Background Redis traffic from asynq (heartbeats, polling, scheduler) was not measured. This can
  matter on Redis providers that bill per command.

## See also

- [Golang ORM](https://github.com/uptrace/bun) for PostgreSQL, MySQL, MSSQL, and SQLite
- [Golang PostgreSQL](https://bun.uptrace.dev/postgres/)
- [Golang HTTP router](https://github.com/uptrace/bunrouter)
- [Golang ClickHouse](https://github.com/uptrace/go-clickhouse)

## Contributors

Thanks to all the people who already contributed!

<a href="https://github.com/vmihailenco/taskq/graphs/contributors">
  <img src="https://contributors-img.web.app/image?repo=vmihailenco/taskq" />
</a>
