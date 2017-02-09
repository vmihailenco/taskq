package msgqueue_test

import (
	"errors"
	"fmt"
	"time"

	"gopkg.in/msgqueue.v1"
	"gopkg.in/msgqueue.v1/memqueue"
)

func timeSince(start time.Time) time.Duration {
	// Truncate.
	return time.Since(start) / time.Second * time.Second
}

func Example_retryOnError() {
	start := time.Now()
	q := memqueue.NewQueue(&msgqueue.Options{
		Handler: func() error {
			fmt.Println(timeSince(start))
			return errors.New("fake error")
		},
		RetryLimit: 3,
		MinBackoff: time.Second,
	})
	defer q.Close()
	q.Processor().Stop()

	q.Call()
	q.Processor().ProcessAll()
	// Output: 0s
	// 1s
	// 3s
}

func Example_messageDelay() {
	start := time.Now()
	q := memqueue.NewQueue(&msgqueue.Options{
		Handler: func() {
			fmt.Println(timeSince(start))
		},
	})
	defer q.Close()
	q.Processor().Stop()

	msg := msgqueue.NewMessage()
	msg.Delay = time.Second
	q.Add(msg)
	q.Processor().ProcessAll()
	// Output: 1s
}
