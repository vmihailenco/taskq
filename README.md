# Golang task/job queue with in-memory, SQS, IronMQ backends

[![Build Status](https://travis-ci.org/go-msgqueue/msgqueue.svg)](https://travis-ci.org/go-msgqueue/msgqueue)

## Installation

```bash
go get -u github.com/go-msgqueue/msgqueue
```

## Features

 - SQS, IronMQ, and in-memory backends.
 - Queue processor can be run on separate server.
 - Automatically scaling number of goroutines used to fetch and process messages.
 - Rate limiting.
 - Global limit of workers.
 - Call once.
 - Automatic retries with exponential backoffs.
 - Automatic pausing when all messages in queue fail.
 - Fallback handler for processing failed messages.
 - Message batching. It is used in SQS and IronMQ backends to add/delete messages in batches.
 - Automatic message compression using Snappy.
 - Statistics.

## Design overview

go-msgqueue is a thin wrapper for SQS and IronMQ clients that uses Redis to implement rate limiting and call once semantic.

go-msgqueue consists of following components:
 - memqueue - in memory queue that can be used for local unit testing.
 - azsqs - Amazon SQS backend.
 - ironmq - IronMQ backend.
 - Manager - provides common interface for creating new queues.
 - Processor - queue processor that works with memqueue, azsqs, and ironmq.

rate limiting is implemented in the processor package using [redis_rate](https://github.com/go-redis/redis_rate). Call once is implemented in clients by checking if message name exists in Redis database.

## API overview

```go
import "github.com/go-msgqueue/msgqueue"
import "github.com/go-redis/redis"
import "golang.org/x/time/rate"

// Create in-memory queue that prints greetings.
q := memqueue.NewQueue(&msgqueue.Options{
    // Handler is automatically retried on error.
    Handler: func(name string) error {
        fmt.Println("Hello", name)
        return nil
    },

    RateLimit: rate.Every(time.Second),

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

// Say "Hello World" once.
for i := 0; i < 100; i++ {
    msg := msgqueue.NewMessage("hello")
    msg.Name = "hello-world"
    q.Add(msg)
}

// Say "Hello World" once with 1 hour delay.
for i := 0; i < 100; i++ {
    msg := msgqueue.NewMessage("hello")
    msg.Name = "hello-world"
    msg.Delay = time.Hour
    q.Add(msg)
}

// Say "Hello World" once in an hour.
for i := 0; i < 100; i++ {
    q.CallOnce(time.Hour, "hello")
}

// Say "Hello World" for Europe region once in an hour.
for i := 0; i < 100; i++ {
    msg := msgqueue.NewMessage("hello")
    msg.SetDelayName(delay, "europe") // set delay and autogenerate message name
    q.Add(msg)
}
```

## SQS, IronMQ, and in-memory queues

SQS, IronMQ, and memqueue share the same API and can be used interchangeably.

### SQS

azsqs package uses Amazon Simple Queue Service as queue backend.

```go
import "github.com/go-msgqueue/msgqueue"
import "github.com/go-msgqueue/msgqueue/azsqs"
import "github.com/aws/aws-sdk-go/service/sqs"

// Create queue.
awsAccountId := "123456789"
q := azsqs.NewQueue(awsSQS(), awsAccountId, &msgqueue.Options{
    Name: "sqs-queue-name",
    Handler: func(name string) error {
        fmt.Println("Hello", name)
        return nil
    },
})

// Same using Manager.
man := azsqs.NewManager(awsSQS(), accountId)
q := man.NewQueue(&msgqueue.Options{...})

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

// Create queue.
q := ironmq.NewQueue(mq.New("ironmq-queue-name"), &msgqueue.Options{
    Handler: func(name string) error {
        fmt.Println("Hello", name)
        return nil
    },
})

// Same using manager.
cfg := iron_config.Config("iron_mq")
man := ironmq.NewManager(&cfg)
q := man.NewQueue(&msgqueue.Options{...})

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

// Create queue.
q := memqueue.NewQueue(&msgqueue.Options{
    Handler: func(name string) error {
        fmt.Println("Hello", name)
        return nil
    },
})

// Same using Manager.
man := memqueue.NewManager()
q := man.NewQueue(&msgqueue.Options{...})

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

## Stats

You can log local queue stats using following code:

```go
func LogQueueStats(q msgqueue.Queue) {
    p := q.Processor()
    opt := p.Options()

    var old *msgqueue.ProcessorStats
    for _ = range time.Tick(3 * time.Second) {
        st := p.Stats()
        if st == nil {
            break
        }

        if old != nil && st.Processed == old.Processed &&
            st.Fails == old.Fails &&
            st.Retries == old.Retries {
            continue
        }
        old = st

        glog.Infof(
            "%s: buffered=%d/%d in_flight=%d/%d "+
                "processed=%d fails=%d retries=%d "+
                "avg_dur=%s min_dur=%s max_dur=%s",
            q, st.Buffered, opt.BufferSize, st.InFlight, opt.WorkerNumber,
            st.Processed, st.Fails, st.Retries,
            st.AvgDuration, st.MinDuration, st.MaxDuration,
        )
    }
}

go LogQueueStats(myqueue)
```

which will log something like this

```
Memqueue<Name=v1-production-notices-add>: buffered=0/1000 in_flight=3/16 processed=16183872 fails=0 retries=0 avg_dur=44.8ms min_dur=100µs max_dur=5.102s
Memqueue<Name=v1-production-notices-add>: buffered=0/1000 in_flight=8/16 processed=16184022 fails=0 retries=0 avg_dur=42ms min_dur=100µs max_dur=5.102s
```
