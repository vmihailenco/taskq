# SQS, IronMQ and in-memory queues with taskqueue-like API

## Quickstart

```go
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

## Using SQS or IronMQ backends

Memqueue, SQS, and IronMQ share the same API and can be used interchangeably:

### SQS

```go
awsAccountId := "123456789"
q := azsqs.NewQueue(awsSQS(), awsAccountId, &queue.Options{
    Name: "sqs-queue-name",
})
```

### IronMQ

```go
q := ironmq.NewQueue(mq.New("ironmq-queue-name"), &queue.Options{})
```
