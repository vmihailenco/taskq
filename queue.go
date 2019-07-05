package taskq

import (
	"fmt"
	"runtime"
	"time"

	"github.com/go-redis/redis_rate"
	"golang.org/x/time/rate"
)

type QueueOptions struct {
	// Queue name.
	Name string

	// Minimum number of goroutines processing messages.
	// Default is 1.
	MinWorkers int
	// Maximum number of goroutines processing messages.
	// Default is 32 * number of CPUs.
	MaxWorkers int
	// Global limit of concurrently running workers across all servers.
	// Overrides MaxWorkers.
	WorkerLimit int
	// Maximum number of goroutines fetching messages.
	// Default is 16 * number of CPUs.
	MaxFetchers int

	// Number of messages reserved by a fetcher in the queue in one request.
	// Default is 10 messages.
	ReservationSize int
	// Time after which the reserved message is returned to the queue.
	// Default is 5 minutes.
	ReservationTimeout time.Duration
	// Time that a long polling receive call waits for a message to become
	// available before returning an empty response.
	// Default is 10 seconds.
	WaitTimeout time.Duration
	// Size of the buffer where reserved messages are stored.
	// Default is the same as ReservationSize.
	BufferSize int

	// Number of consecutive failures after which queue processing is paused.
	// Default is 100 failures.
	PauseErrorsThreshold int

	// Processing rate limit.
	RateLimit rate.Limit

	// Optional rate limiter interface. The default is to use Redis.
	RateLimiter RateLimiter

	// Redis client that is used for storing metadata.
	Redis Redis

	// Optional storage interface. The default is to use Redis.
	Storage Storage

	inited bool
}

func (opt *QueueOptions) Init() {
	if opt.inited {
		return
	}
	opt.inited = true

	if opt.Name == "" {
		panic("QueueOptions.Name is required")
	}
	if opt.WorkerLimit > 0 {
		opt.MaxWorkers = opt.WorkerLimit
	}
	if opt.MinWorkers == 0 {
		opt.MinWorkers = 1
	}
	if opt.MaxWorkers == 0 {
		opt.MaxWorkers = 32 * runtime.NumCPU()
	}
	if opt.MaxFetchers == 0 {
		opt.MaxFetchers = 8 * runtime.NumCPU()
	}

	switch opt.PauseErrorsThreshold {
	case -1:
		opt.PauseErrorsThreshold = 0
	case 0:
		opt.PauseErrorsThreshold = 100
	}

	if opt.RateLimit == 0 {
		opt.RateLimit = rate.Inf
	}

	if opt.ReservationSize == 0 {
		opt.ReservationSize = 10
	}
	if opt.ReservationTimeout == 0 {
		opt.ReservationTimeout = 5 * time.Minute
	}
	if opt.BufferSize == 0 {
		opt.BufferSize = opt.ReservationSize
	}
	if opt.WaitTimeout == 0 {
		opt.WaitTimeout = 10 * time.Second
	}

	if opt.Storage == nil {
		opt.Storage = newRedisStorage(opt.Redis)
	}

	if opt.RateLimit != rate.Inf && opt.RateLimiter == nil && opt.Redis != nil {
		limiter := redis_rate.NewLimiter(opt.Redis)
		limiter.Fallback = rate.NewLimiter(opt.RateLimit, 1)
		opt.RateLimiter = limiter
	}
}

//------------------------------------------------------------------------------

type Queuer interface {
	fmt.Stringer
	Name() string
	Options() *QueueOptions
	Consumer() *Consumer

	Len() (int, error)
	Add(msg *Message) error
	ReserveN(n int, waitTimeout time.Duration) ([]Message, error)
	Release(msg *Message) error
	Delete(msg *Message) error
	Purge() error
	Close() error
	CloseTimeout(timeout time.Duration) error
}

type Queue struct {
	q   Queuer
	opt *QueueOptions
}

var _ Queuer = (*Queue)(nil)

func RegisterQueue(opt *QueueOptions) *Queue {
	q := &Queue{
		opt: opt,
	}
	if err := Queues.Register(q); err != nil {
		panic(err)
	}
	return q
}

func (q *Queue) Bind(factory Factory) {
	q.q = factory.NewQueue(q.opt)
}

func (q *Queue) String() string {
	return fmt.Sprintf("queue=%q", q.Name())
}

func (q *Queue) Name() string {
	return q.opt.Name
}

func (q *Queue) Options() *QueueOptions {
	return q.q.Options()
}

func (q *Queue) Consumer() *Consumer {
	return q.q.Consumer()
}

func (q *Queue) Len() (int, error) {
	return q.q.Len()
}

func (q *Queue) Add(msg *Message) error {
	return q.q.Add(msg)
}

func (q *Queue) ReserveN(n int, waitTimeout time.Duration) ([]Message, error) {
	return q.q.ReserveN(n, waitTimeout)
}

func (q *Queue) Release(msg *Message) error {
	return q.q.Release(msg)
}

func (q *Queue) Delete(msg *Message) error {
	return q.q.Delete(msg)
}

func (q *Queue) Purge() error {
	return q.q.Purge()
}

func (q *Queue) Close() error {
	return q.q.Close()
}

func (q *Queue) CloseTimeout(timeout time.Duration) error {
	return q.q.CloseTimeout(timeout)
}
