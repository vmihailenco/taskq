<p align="center">
  <a href="https://uptrace.dev/?utm_source=gh-taskq&utm_campaign=gh-taskq-banner1">
    <img src="https://raw.githubusercontent.com/uptrace/roadmap/master/banner1.png">
  </a>
</p>

# Golang asynchronous task/job queue with Redis, SQS, IronMQ, and in-memory backends

[![Build Status](https://travis-ci.org/vmihailenco/taskq.svg)](https://travis-ci.org/vmihailenco/taskq)
[![GoDoc](https://godoc.org/github.com/vmihailenco/taskq?status.svg)](https://pkg.go.dev/github.com/vmihailenco/taskq/v3?tab=doc)

## Installation

taskq supports 2 last Go versions and requires a Go version with
[modules](https://github.com/golang/go/wiki/Modules) support. So make sure to
initialize a Go module:

```shell
go mod init github.com/my/repo
```

And then install taskq/v3 (note _v3_ in the import; omitting it is a popular
mistake):

```shell
go get github.com/vmihailenco/taskq/v3
```

## Features

- Redis, SQS, IronMQ, and in-memory backends.
- Automatically scaling number of goroutines used to fetch (fetcher) and process
  messages (worker).
- Global rate limiting.
- Global limit of workers.
- Call once - deduplicating messages with same name.
- Automatic retries with exponential backoffs.
- Automatic pausing when all messages in queue fail.
- Fallback handler for processing failed messages.
- Message batching. It is used in SQS and IronMQ backends to add/delete messages
  in batches.
- Automatic message compression using snappy / s2.

## Quickstart

I recommend that you split your app into the two parts:

- An API that accepts requests from customers and adds tasks to the queues.
- A Worker that fetches tasks from the queues and processes them.

This way you can:

- Isolate API and worker from each other.
- Scale API and worker separately.
- Have different configs for API and worker (like timeouts).

There is an [api_worker example](example/api_worker) that demonstrates this
approach using Redis as a backend:

```bash
cd example/api_worker
go run worker/worker.go
go run api/api.go
```

You start by choosing a backend to use - in our case Redis:

```go
package api_worker

var QueueFactory = redisq.NewFactory()
```

Using that factory you create a queue that contains tasks:

```go
var MainQueue = QueueFactory.RegisterQueue(&taskq.QueueOptions{
    Name:  "api-worker",
    Redis: Redis, // go-redis client
})
```

Using the queue you create a task with handler that does some useful work:

```go
var CountTask = taskq.RegisterTask(&taskq.TaskOptions{
    Name: "counter",
    Handler: func() error {
        IncrLocalCounter()
        return nil
    },
})
```

Then in an API binary you use tasks to add messages/jobs to queues:

```go
ctx := context.Background()
for {
    // call task handler without any args
    err := api_worker.MainQueue.Add(api_worker.CountTask.WithArgs(ctx))
    if err != nil {
        log.Fatal(err)
    }
}
```

And in a worker binary you start processing queues:

```go
err := api_worker.MainQueue.Start(context.Background())
if err != nil {
    log.Fatal(err)
}
```

## API overview

```go
t := myQueue.RegisterTask(&taskq.TaskOptions{
    Name:    "greeting",
    Handler: func(name string) error {
        fmt.Println("Hello", name)
        return nil
    },
})

// Say "Hello World".
err := myQueue.Add(t.WithArgs(context.Background(), "World"))
if err != nil {
    panic(err)
}

// Say "Hello World" with 1 hour delay.
msg := t.WithArgs(ctx, "World")
msg.Delay = time.Hour
_ = myQueue.Add(msg)

// Say "Hello World" once.
for i := 0; i < 100; i++ {
    msg := t.WithArgs(ctx, "World")
    msg.Name = "hello-world" // unique
    _ = myQueue.Add(msg)
}

// Say "Hello World" once with 1 hour delay.
for i := 0; i < 100; i++ {
    msg := t.WithArgs(ctx, "World")
    msg.Name = "hello-world"
    msg.Delay = time.Hour
    _ = myQueue.Add(msg)
}

// Say "Hello World" once in an hour.
for i := 0; i < 100; i++ {
    msg := t.WithArgs(ctx, "World").OnceInPeriod(time.Hour)
    _ = myQueue.Add(msg)
}

// Say "Hello World" for Europe region once in an hour.
for i := 0; i < 100; i++ {
    msg := t.WithArgs(ctx, "World").OnceInPeriod(time.Hour, "World", "europe")
    _ = myQueue.Add(msg)
}
```

## Message deduplication

If a `Message` has a `Name` then this will be used as unique identifier and
messages with the same name will be deduplicated (i.e. not processed again)
within a 24 hour period (or possibly longer if not evicted from local cache
after that period). Where `Name` is omitted then non deduplication occurs and
each message will be processed. `Task`'s `WithMessage` and `WithArgs` both
produces messages with no `Name` so will not be deduplicated. `OnceWithArgs`
sets a name based off a consistent hash of the arguments and a quantised period
of time (i.e. 'this hour', 'today') passed to `OnceWithArgs` a `period`. This
guarantees that the same function will not be called with the same arguments
during `period'.

## Handlers

A `Handler` and `FallbackHandler` are supplied to `RegisterTask` in the
`TaskOptions`.

There are three permitted types of signature:

1. A zero-argument function
2. A function whose arguments are assignable in type from those which are passed
   in the message
3. A function which takes a single `*Message` argument

If a task is registered with a handler that takes a Go `context.Context` as its
first argument then when that handler is invoked it will be passed the same
`Context` that was passed to `Consumer.Start(ctx)`. This can be used to transmit
a signal to abort to all tasks being processed:

```go
var AbortableTask = MainQueue.RegisterTask(&taskq.TaskOptions{
    Name: "SomethingLongwinded",
    Handler: func(ctx context.Context) error {
        for range time.Tick(time.Second) {
            select {
                case <-ctx.Done():
                    return ctx.Err()
                default:
                    fmt.Println("Wee!")
            }
        }
        return nil
    },
})

```

## Custom message delay

If error returned by handler implements `Delay() time.Duration` interface then
that delay is used to postpone message processing.

```go
type RateLimitError string

func (e RateLimitError) Error() string {
    return string(e)
}

func (RateLimitError) Delay() time.Duration {
    return time.Hour
}

func handler() error {
    return RateLimitError("calm down")
}
```

## Tracing

taskq supports tracing out-of-the-box using
[OpenTelemetry](https://opentelemetry.io/) API. To instrument a queue, use the
following code:

```go
import "github.com/vmihailenco/taskq/extra/taskqotel"

consumer := queue.Consumer()
consumer.AddHook(&taskqotel.OpenTelemetryHook{})
```

or using a `taskq.Factory`:

```go
factory.Range(func(q taskq.Queue) bool {
    consumer := q.Consumer()
    consumer.AddHook(&taskqext.OpenTelemetryHook{})

    return true
})
```

We recommend using [Uptrace.dev](https://github.com/uptrace/uptrace-go) as a
tracing backend.
