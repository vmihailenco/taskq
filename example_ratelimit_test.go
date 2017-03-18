package msgqueue_test

import (
	"fmt"
	"time"

	"github.com/go-msgqueue/msgqueue"
	"github.com/go-msgqueue/msgqueue/memqueue"
)

type RateLimitError string

func (e RateLimitError) Error() string {
	return string(e)
}

func (RateLimitError) Delay() time.Duration {
	return 2 * time.Second
}

func Example_rateLimit() {
	start := time.Now()
	q := memqueue.NewQueue(&msgqueue.Options{
		Handler: func() error {
			fmt.Println(timeSince(start))
			return RateLimitError("calm down")
		},
		RetryLimit: 2,
		MinBackoff: time.Millisecond,
	})
	defer q.Close()
	q.Processor().Stop()

	q.Call()
	q.Processor().ProcessAll()
	// Output: 0s
	// 2s
}
