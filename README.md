# SQS & IronMQ clients with rate-limiting and call once

## Installation

```bash
go get -u gopkg.in/queue.v1
```

## Design

go-queue is a thin wrapper for SQS and IronMQ clients that uses Redis to implement rate-limiting and call once semantic.

## API overview

```go
// Create in-memory queue that prints greetings.
q := memqueue.NewQueue(&queue.Options{
    // Handler is retried on error.
    Handler: func(name string) error {
        fmt.Println("Hello", name)
        return nil
    },
})

// Invoke handler with arguments
q.Call("World")

// Same using Message API
q.Add(queue.NewMessage("World"))

// Say "Hello World" with 1 hour delay
msg := queue.NewMessage("World")
msg.Delay = time.Hour
q.Add(msg)

// Say "Hello World" only once
for i := 0; i < 100; i++ {
    msg := queue.NewMessage("hello")
    msg.Name = "hello-world"
    q.Add(msg)
}

// Say "Hello World" only once with 1 hour delay
for i := 0; i < 100; i++ {
    msg := queue.NewMessage("hello")
    msg.Name = "hello-world"
    msg.Delay = time.Hour
    q.Add(msg)
}

// Say "Hello World" only once with 1 hour delay
for i := 0; i < 100; i++ {
    q.CallOnce(time.Hour, "hello")
}

// Say "Hello World" for Europe region only once with 1 hour delay
for i := 0; i < 100; i++ {
    msg := queue.NewMessage("hello")
    msg.SetDelayName(delay, "europe") // autogenerates message name from args
    q.Add(msg)
}
```

## SQS / IronMQ / memqueue

SQS, IronMQ, and memqueue share the same API and can be used interchangeably.

### SQS

azsqs package uses Amazon Simple Queue Service.

```go
import "gopkg.in/queue.v1"
import "gopkg.in/queue.v1/azsqs"
import "github.com/aws/aws-sdk-go/service/sqs"

awsAccountId := "123456789"
q := azsqs.NewQueue(awsSQS(), awsAccountId, &queue.Options{
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

ironmq package uses IronMQ.

```go
import "gopkg.in/queue.v1"
import "gopkg.in/queue.v1/ironmq"
import "github.com/iron-io/iron_go3/mq"

q := ironmq.NewQueue(mq.New("ironmq-queue-name"), &queue.Options{
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

### Memqueue

Memqueue is in-memory implementation primarily useful for local development / running tests. Unlike SQS and IronMQ it has running queue processor by default.

```go
import "gopkg.in/queue.v1"

q := memqueue.NewQueue(&queue.Options{
    Handler: func(name string) error {
        fmt.Println("Hello", name)
        return nil
    },
})
```
