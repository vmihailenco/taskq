package queue

import (
	"runtime"
	"time"

	timerate "golang.org/x/time/rate"
	"gopkg.in/go-redis/rate.v4"
	"gopkg.in/redis.v4"
)

type Redis interface {
	SetNX(string, interface{}, time.Duration) *redis.BoolCmd
	SAdd(key string, members ...interface{}) *redis.IntCmd
	SMembers(key string) *redis.StringSliceCmd
	Pipelined(func(pipe *redis.Pipeline) error) ([]redis.Cmder, error)
	Publish(channel, message string) *redis.IntCmd
}

type Storage interface {
	Exists(key string) bool
}

type RateLimiter interface {
	AllowRate(name string, limit timerate.Limit) (delay time.Duration, allow bool)
}

type Options struct {
	// Queue name.
	Name string

	Handler         interface{}
	FallbackHandler interface{}

	// Number of goroutines processing messages.
	WorkerNumber int

	// Number of scavengers deleting messages.
	ScavengerNumber int

	// Messages are prefetched and stored in buffer of this size.
	BufferSize int

	// Time after which the reserved message is returned to the queue.
	ReservationTimeout time.Duration

	// Number of tries/releases after which the message fails permanently
	// and is deleted.
	RetryLimit int

	// Minimum time between retries.
	MinBackoff time.Duration

	// Processing rate limit.
	RateLimit timerate.Limit

	// Redis client that is used for storing messages metadata.
	Redis Redis

	// Optional storage interface.
	Storage Storage

	// Optional rate limiter interface.
	RateLimiter RateLimiter

	inited bool
}

func (opt *Options) Init() {
	if opt.inited {
		return
	}
	opt.inited = true

	if opt.WorkerNumber == 0 {
		opt.WorkerNumber = 10 * runtime.NumCPU()
	}
	if opt.ScavengerNumber == 0 {
		opt.ScavengerNumber = runtime.NumCPU() + 1
	}
	if opt.BufferSize == 0 {
		opt.BufferSize = opt.WorkerNumber
		if opt.BufferSize > 10 {
			opt.BufferSize = 10
		}
	}
	if opt.RateLimit == 0 {
		opt.RateLimit = timerate.Inf
	}
	if opt.ReservationTimeout == 0 {
		opt.ReservationTimeout = 300 * time.Second
	}
	if opt.RetryLimit == 0 {
		opt.RetryLimit = 10
	}
	if opt.MinBackoff == 0 {
		opt.MinBackoff = 3 * time.Second
	}

	if opt.Storage == nil {
		opt.Storage = storage{opt.Redis}
	}

	if opt.RateLimit != timerate.Inf && opt.RateLimiter == nil && opt.Redis != nil {
		fallbackLimiter := timerate.NewLimiter(opt.RateLimit, 1)
		opt.RateLimiter = rate.NewLimiter(opt.Redis, fallbackLimiter)
	}
}

type storage struct {
	Redis
}

func (s storage) Exists(key string) bool {
	return !s.SetNX(key, "", 24*time.Hour).Val()
}
