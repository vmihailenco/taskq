package taskq_test

import (
	"fmt"
	"time"

	"github.com/vmihailenco/taskq"
	"github.com/vmihailenco/taskq/memqueue"
)

type RateLimitError string

func (e RateLimitError) Error() string {
	return string(e)
}

func (RateLimitError) Delay() time.Duration {
	return 3 * time.Second
}

func Example_customRateLimit() {
	start := time.Now()
	q := memqueue.NewQueue(&taskq.QueueOptions{})
	task := q.NewTask(&taskq.TaskOptions{
		Handler: func() error {
			fmt.Println("retried in", timeSince(start))
			return RateLimitError("calm down")
		},
		RetryLimit: 2,
		MinBackoff: time.Millisecond,
	})

	task.Call()

	// Wait for all messages to be processed.
	_ = q.Close()

	// Output: retried in 0s
	// retried in 3s
}
