package taskq_test

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/go-redis/redis_rate/v8"
	"github.com/vmihailenco/taskq/v3"
	"github.com/vmihailenco/taskq/v3/memqueue"
)

func timeSince(start time.Time) time.Duration {
	secs := float64(time.Since(start)) / float64(time.Second)
	return time.Duration(math.Floor(secs)) * time.Second
}

func timeSinceCeil(start time.Time) time.Duration {
	secs := float64(time.Since(start)) / float64(time.Second)
	return time.Duration(math.Ceil(secs)) * time.Second
}

func Example_retryOnError() {
	start := time.Now()
	q := memqueue.NewQueue(&taskq.QueueOptions{
		Name: "test",
	})
	task := taskq.RegisterTask(&taskq.TaskOptions{
		Name: "Example_retryOnError",
		Handler: func() error {
			fmt.Println("retried in", timeSince(start))
			return errors.New("fake error")
		},
		RetryLimit: 3,
		MinBackoff: time.Second,
	})

	ctx := context.Background()
	q.Add(task.WithArgs(ctx))

	// Wait for all messages to be processed.
	_ = q.Close()

	// Output: retried in 0s
	// retried in 1s
	// retried in 3s
}

func Example_messageDelay() {
	start := time.Now()
	q := memqueue.NewQueue(&taskq.QueueOptions{
		Name: "test",
	})
	task := taskq.RegisterTask(&taskq.TaskOptions{
		Name: "Example_messageDelay",
		Handler: func() {
			fmt.Println("processed with delay", timeSince(start))
		},
	})

	ctx := context.Background()
	msg := task.WithArgs(ctx)
	msg.Delay = time.Second
	_ = q.Add(msg)

	// Wait for all messages to be processed.
	_ = q.Close()

	// Output: processed with delay 1s
}

func Example_rateLimit() {
	start := time.Now()
	q := memqueue.NewQueue(&taskq.QueueOptions{
		Name:      "test",
		Redis:     redisRing(),
		RateLimit: redis_rate.PerSecond(1),
	})
	task := taskq.RegisterTask(&taskq.TaskOptions{
		Name:    "Example_rateLimit",
		Handler: func() {},
	})

	const n = 5

	ctx := context.Background()
	for i := 0; i < n; i++ {
		_ = q.Add(task.WithArgs(ctx))
	}

	// Wait for all messages to be processed.
	_ = q.Close()

	fmt.Printf("%d msg/s", timeSinceCeil(start)/time.Second/n)
	// Output: 1 msg/s
}

func Example_once() {
	q := memqueue.NewQueue(&taskq.QueueOptions{
		Name:      "test",
		Redis:     redisRing(),
		RateLimit: redis_rate.PerSecond(1),
	})
	task := taskq.RegisterTask(&taskq.TaskOptions{
		Name: "Example_once",
		Handler: func(name string) {
			fmt.Println("hello", name)
		},
	})

	ctx := context.Background()
	for i := 0; i < 10; i++ {
		msg := task.WithArgs(ctx, "world")
		// Call once in a second.
		msg.OnceInPeriod(time.Second)

		_ = q.Add(msg)
	}

	// Wait for all messages to be processed.
	_ = q.Close()

	// Output: hello world
}
