# Golang task/job queue with Redis, SQS, IronMQ, and in-memory backends

[![Build Status](https://travis-ci.org/vmihailenco/taskq.svg)](https://travis-ci.org/vmihailenco/taskq)
[![GoDoc](https://godoc.org/github.com/vmihailenco/taskq?status.svg)](https://godoc.org/github.com/vmihailenco/taskq)

## Installation

```bash
go get -u github.com/vmihailenco/taskq
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
 - Automatic message compression using zstd or snappy.

## Quickstart

I recommend to split your app into 2 parts:
- API that accepts requests from customers and adds tasks to the queues.
- Worker that fetches tasks from the queues and processes them.

This way you can:
- isolate API and worker from each other;
- scale API and worker separately;
- have different configs (like timeouts).

There is an [api_worker example](examples/api_worker) that does exactly that and can be run:

```bash
cd examples/api_worker
go run worker/main.go
go run api/main.go
```

First you need to define queue and task in that queue:

```go
var (
	QueueFactory = redisq.NewFactory()
	MainQueue    = QueueFactory.NewQueue(&taskq.QueueOptions{
		Name:  "api-worker",
		Redis: Redis,
	})
	CountTask = MainQueue.NewTask(&taskq.TaskOptions{
		Name: "counter",
		Handler: func() error {
			IncrLocalCounter()
			return nil
		},
	})
)
```

API that adds tasks:

``` go
for {
	err := api_worker.CountTask.Call()
	if err != nil {
		log.Fatal(err)
	}
	api_worker.IncrLocalCounter()
}
```

Worker that processes tasks:

``` go
err := api_worker.QueueFactory.StartConsumers()
if err != nil {
	log.Fatal(err)
}
```

## API overview

```go
t := myQueue.NewTask(&taskq.TaskOptions{
	Name: "greeting",
	Handler: func(name string) error {
		fmt.Println("Hello", name)
		return nil
	},
})

// Say "Hello World".
t.Call("World")

// Same using Message API.
t.AddMessage(taskq.NewMessage("World"))

// Say "Hello World" with 1 hour delay.
msg := taskq.NewMessage("World")
msg.Delay = time.Hour
t.AddMessage(msg)

// Say "Hello World" once.
for i := 0; i < 100; i++ {
    msg := taskq.NewMessage("hello")
    msg.Name = "hello-world" // unique
    t.Add(msg)
}

// Say "Hello World" once with 1 hour delay.
for i := 0; i < 100; i++ {
    msg := taskq.NewMessage("hello")
    msg.Name = "hello-world"
    msg.Delay = time.Hour
    t.Add(msg)
}

// Say "Hello World" once in an hour.
for i := 0; i < 100; i++ {
    t.CallOnce(time.Hour, "hello")
}

// Say "Hello World" for Europe region once in an hour.
for i := 0; i < 100; i++ {
    msg := taskq.NewMessage("hello")
    msg.OnceWithArgs(time.Hour, "europe") // set delay and autogenerate message name
    t.Add(msg)
}
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
