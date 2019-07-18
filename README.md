# Golang asynchronous task/job queue with Redis, SQS, IronMQ, and in-memory backends

[![Build Status](https://travis-ci.org/vmihailenco/taskq.svg)](https://travis-ci.org/vmihailenco/taskq)
[![GoDoc](https://godoc.org/github.com/vmihailenco/taskq?status.svg)](https://godoc.org/github.com/vmihailenco/taskq)

## Installation

```bash
go get github.com/vmihailenco/taskq/v2
```

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
 - Automatic message compression using zstd.

## Quickstart

I recommend that you split your app into two parts:
- An API that accepts requests from customers and adds tasks to the queues.
- A Worker that fetches tasks from the queues and processes them.

This way you can:
- Isolate API and worker from each other;
- Scale API and worker separately;
- Have different configs for API and worker (like timeouts).

There is an [api_worker example](examples/api_worker) that demonstrates this approach using Redis as backend:

```bash
cd examples/api_worker
go run worker/main.go
go run api/main.go
```

You start by choosing backend to use - in our case Redis:

```go
package api_worker

var QueueFactory = redisq.NewFactory()
```

Using that factory you create queue that contains task(s):

```go
var MainQueue = QueueFactory.RegisterQueue(&taskq.QueueOptions{
	Name:  "api-worker",
	Redis: Redis, // go-redis client
})
```

Using the queue you create task with handler that does some useful work:

```go
var CountTask = MainQueue.RegisterTask(&taskq.TaskOptions{
	Name: "counter",
	Handler: func() error {
		IncrLocalCounter()
		return nil
	},
})
```

Then in API you use the task to add messages/jobs to the queues:

```go
for {
	// call task handler without any args
	err := api_worker.MainQueue.Add(api_worker.CountTask.WithArgs(context.Background())) 
	if err != nil {
		log.Fatal(err)
	}
}
```

And in worker you start processing the queue:

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
myQueue.Add(t.WithArgs(context.Background(), "World"))

// Same using Message API.
myQueue.Add(t.WithMessage(taskq.NewMessage("World")))

// Say "Hello World" with 1 hour delay.
msg := taskq.NewMessage("World")
msg.Delay = time.Hour
myQueue.Add(msg)

// Say "Hello World" once.
for i := 0; i < 100; i++ {
    msg := taskq.NewMessage("hello")
    msg.Name = "hello-world" // unique
    myQueue.Add(msg)
}

// Say "Hello World" once with 1 hour delay.
for i := 0; i < 100; i++ {
    msg := taskq.NewMessage("hello")
    msg.Name = "hello-world"
    msg.Delay = time.Hour
    myQueue.Add(msg)
}

// Say "Hello World" once in an hour.
for i := 0; i < 100; i++ {
    myQueue.Add(t.OnceWithArgs(time.Hour, "hello"))
}

// Say "Hello World" for Europe region once in an hour.
for i := 0; i < 100; i++ {
    msg := taskq.NewMessage("hello")
    msg.OnceWithArgs(time.Hour, "europe") // set delay and autogenerate message name
    myQueue.Add(msg)
}
```

## Message deduplication
If a `Message` has a `Name` then this will be used as unique identifier and messages with the same name will be deduplicated (i.e. not processed again) within a 24 hour period (or possibly longer if not evicted from local cache after that period). Where `Name` is omitted then non deduplication occurs and each message will be processed. `Task`'s `WithMessage` and `WithArgs` both produces messages with no `Name` so will not be deduplicated. `OnceWithArgs` sets a name based off a consistent hash of the arguments and a quantised period of time (i.e. 'this hour', 'today') passed to `OnceWithArgs` a `period`. This guarantees that the same function will not be called with the same arguments during `period'.

## Contextual handlers
If a task is registered with a handler that takes a Go `context.Context` as its first argument then when that handler is invoked it will be passed the same `Context` that was based to `Consumer.Start(ctx)`. This can be used to transmit a signal to abort to all tasks being processed:

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

If error returned by handler implements `Delay() time.Duration` interface then that delay is used to postpone message processing.

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
