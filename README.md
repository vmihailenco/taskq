# SQS, IronMQ and in-memory queues with taskqueue-like API

## Memqueue

```go
q := memqueue.NewQueue(&queue.Options{
    // Handlers is retried on error.
    Handler: func(str string, num int) error {
        fmt.Println(str, num)
        return nil
    },
})

// Call handler with arguments
q.Call("hello", 42)

// Same using Message
q.Add(queue.NewMessage("hello", 42))

// Call handler with 1 hour delay
msg := queue.NewMessage("hello", 42)
msg.Delay = time.Hour
q.Add(msg)

// Call handler with such arguments at most once in an hour
q.CallOnce(time.Hour, "hello", 42)

// Same using Message
msg := queue.NewMessage("hello", 42)
msg.SetDelayName(time.Hour, "hello", 42)
q.Add(msg)
```
