# SQS & IronMQ clients with rate limiting and call once [![Build Status](https://travis-ci.org/go-msgqueue/msgqueue.svg?branch=v1)](https://travis-ci.org/go-msgqueue/msgqueue)

## Installation

```bash
go get -u github.com/go-msgqueue/msgqueue
```

## Features

 - SQS, IronMQ, and in-memory clients.
 - Queue processor can be run on separate server.
 - Rate limiting.
 - Call once.
 - Automatic retries with exponential backoffs.
 - Automatic pausing when all messages in queue fail.
 - Fallback handler for processing failed messages.
 - Processed messages are deleted in batches.
 - Statistics.

## Design overview

go-msgqueue is a thin wrapper for SQS and IronMQ clients that uses Redis to implement rate limiting and call once semantic.

go-msgqueue consists of following packages:
 - memqueue - in memory queue that can be used for local unit testing.
 - azsqs - Amazon SQS client.
 - ironmq - IronMQ client.
 - processor - queue processor that works with memqueue, azsqs, and ironmq.

rate limiting is implemented in the processor package using [go-redis rate](https://github.com/go-redis/rate). Call once is implemented in the clients by checking if key that consists of message name exists in Redis database.

## API overview

```go
import "github.com/go-msgqueue/msgqueue"
import "github.com/go-redis/redis"
import timerate "golang.org/x/time/rate"

// Create in-memory queue that prints greetings.
q := memqueue.NewQueue(&msgqueue.Options{
    // Handler is automatically retried on error.
    Handler: func(name string) error {
        fmt.Println("Hello", name)
        return nil
    },

    RateLimit: timerate.Every(time.Second),

    // Redis is only needed for rate limiting and call once.
    Redis: redis.NewClient(&redis.Options{
        Addr: ":6379",
    }),
})

// Invoke handler with arguments.
q.Call("World")

// Same using Message API.
q.Add(msgqueue.NewMessage("World"))

// Say "Hello World" with 1 hour delay.
msg := msgqueue.NewMessage("World")
msg.Delay = time.Hour
q.Add(msg)

// Say "Hello World" only once.
for i := 0; i < 100; i++ {
    msg := msgqueue.NewMessage("hello")
    msg.Name = "hello-world"
    q.Add(msg)
}

// Say "Hello World" only once with 1 hour delay.
for i := 0; i < 100; i++ {
    msg := msgqueue.NewMessage("hello")
    msg.Name = "hello-world"
    msg.Delay = time.Hour
    q.Add(msg)
}

// Same using CallOnce.
for i := 0; i < 100; i++ {
    q.CallOnce(time.Hour, "hello")
}

// Say "Hello World" for Europe region only once with 1 hour delay.
for i := 0; i < 100; i++ {
    msg := msgqueue.NewMessage("hello")
    msg.SetDelayName(delay, "europe") // set delay & autogenerate message name
    q.Add(msg)
}
```

## SQS & IronMQ & in-memory queues

SQS, IronMQ, and memqueue share the same API and can be used interchangeably.

### SQS

azsqs package uses Amazon Simple Queue Service as queue backend.

```go
import "github.com/go-msgqueue/msgqueue"
import "github.com/go-msgqueue/msgqueue/azsqs"
import "github.com/aws/aws-sdk-go/service/sqs"

awsAccountId := "123456789"
q := azsqs.NewQueue(awsSQS(), awsAccountId, &msgqueue.Options{
    Name: "sqs-queue-name",
    Handler: func(name string) error {
        fmt.Println("Hello", name)
        return nil
    },
})

// Add message.
q.Call("World")

// Start processing queue.
p := q.Processor()
p.Start()

// Stop processing.
p.Stop()
```

### IronMQ

ironmq package uses IronMQ as queue backend.

```go
import "github.com/go-msgqueue/msgqueue"
import "github.com/go-msgqueue/msgqueue/ironmq"
import "github.com/iron-io/iron_go3/mq"

q := ironmq.NewQueue(mq.New("ironmq-queue-name"), &msgqueue.Options{
    Handler: func(name string) error {
        fmt.Println("Hello", name)
        return nil
    },
})

// Add message.
q.Call("World")

// Start processing queue.
p := q.Processor()
p.Start()

// Stop processing.
p.Stop()
```

### In-memory

memqueue is in-memory queue backend implementation primarily useful for local development / unit testing. Unlike SQS and IronMQ it has running queue processor by default.

```go
import "github.com/go-msgqueue/msgqueue"

q := memqueue.NewQueue(&msgqueue.Options{
    Handler: func(name string) error {
        fmt.Println("Hello", name)
        return nil
    },
})

// Stop processor if you don't need it.
p := q.Processor()
p.Stop()

// Process one message.
err := p.ProcessOne()

// Process all buffered messages.
err := p.ProcessAll()
```

## Custom message delay

If error returned by handler implements `Delay() time.Duration` that delay is used to postpone message processing.

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

q := memqueue.NewQueue(&msgqueue.Options{
    Handler: handler,
})
```
