package msgqueue_test

import (
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/go-msgqueue/msgqueue"
	"github.com/go-msgqueue/msgqueue/memqueue"

	"github.com/go-redis/redis"
	timerate "golang.org/x/time/rate"
)

func redisRing() *redis.Ring {
	ring := redis.NewRing(&redis.RingOptions{
		Addrs:    map[string]string{"0": ":6379"},
		PoolSize: 100,
	})
	err := ring.FlushDb().Err()
	if err != nil {
		panic(err)
	}
	return ring
}

func timeSince(start time.Time) time.Duration {
	secs := float64(time.Since(start)) / float64(time.Second)
	return time.Duration(math.Floor(secs)) * time.Second
}

func Example_retryOnError() {
	start := time.Now()
	q := memqueue.NewQueue(&msgqueue.Options{
		Handler: func() error {
			fmt.Println("retried in", timeSince(start))
			return errors.New("fake error")
		},
		RetryLimit: 3,
		MinBackoff: time.Second,
	})
	defer q.Close()
	q.Processor().Stop()

	q.Call()
	q.Processor().ProcessAll()
	// Output: retried in 0s
	// retried in 1s
	// retried in 3s
}

func Example_messageDelay() {
	start := time.Now()
	q := memqueue.NewQueue(&msgqueue.Options{
		Handler: func() {
			fmt.Println("processed with delay", timeSince(start))
		},
	})
	defer q.Close()
	q.Processor().Stop()

	msg := msgqueue.NewMessage()
	msg.Delay = time.Second
	q.Add(msg)
	q.Processor().ProcessAll()
	// Output: processed with delay 1s
}

func Example_rateLimit() {
	start := time.Now()
	q := memqueue.NewQueue(&msgqueue.Options{
		Handler:   func() {},
		Redis:     redisRing(),
		RateLimit: timerate.Every(time.Second),
	})

	for i := 0; i < 3; i++ {
		q.Call()
	}

	// Close queue to make sure all messages are processed.
	_ = q.Close()

	fmt.Println("3 messages processed in", timeSince(start))
	// Output: 3 messages processed in 2s
}

func Example_once() {
	q := memqueue.NewQueue(&msgqueue.Options{
		Handler: func(name string) {
			fmt.Println("hello", name)
		},
		Redis:     redisRing(),
		RateLimit: timerate.Every(time.Second),
	})

	for _, name := range []string{"world", "adele"} {
		for i := 0; i < 10; i++ {
			// Call once in a second.
			q.CallOnce(time.Second, name)
		}
	}

	// Close queue to make sure all messages are processed.
	_ = q.Close()

	// Output: hello world
	// hello adele
}

func Example_maxWorkers() {
	start := time.Now()
	q := memqueue.NewQueue(&msgqueue.Options{
		Handler: func() {
			fmt.Println(timeSince(start))
			time.Sleep(time.Second)
		},
		Redis:      redisRing(),
		MaxWorkers: 1,
	})

	for i := 0; i < 3; i++ {
		q.Call()
	}

	// Close queue to make sure all messages are processed.
	_ = q.Close()

	// Output: 0s
	// 1s
	// 2s
}
