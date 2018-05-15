package msgqueue

import (
	"runtime"
	"time"

	"github.com/go-redis/redis"
	"github.com/go-redis/redis_rate"
	"golang.org/x/time/rate"
)

type Redis interface {
	Del(keys ...string) *redis.IntCmd
	SetNX(key string, value interface{}, expiration time.Duration) *redis.BoolCmd
	SAdd(key string, members ...interface{}) *redis.IntCmd
	SMembers(key string) *redis.StringSliceCmd
	Pipelined(func(pipe redis.Pipeliner) error) ([]redis.Cmder, error)
	Eval(script string, keys []string, args ...interface{}) *redis.Cmd
	EvalSha(sha1 string, keys []string, args ...interface{}) *redis.Cmd
	ScriptExists(scripts ...string) *redis.BoolSliceCmd
	ScriptLoad(script string) *redis.StringCmd
	Publish(channel string, message interface{}) *redis.IntCmd
}

type Storage interface {
	Exists(key string) bool
}

type redisStorage struct {
	Redis
}

var _ Storage = (*redisStorage)(nil)

func (s redisStorage) Exists(key string) bool {
	val, err := s.SetNX(key, "", 24*time.Hour).Result()
	if err != nil {
		return true
	}
	return !val
}

type RateLimiter interface {
	AllowRate(name string, limit rate.Limit) (delay time.Duration, allow bool)
}

type Options struct {
	// Queue name.
	Name string
	// Queue group name.
	GroupName string

	// Function called to process a message.
	Handler interface{}
	// Function called to process failed message.
	FallbackHandler interface{}

	// Maximum number of goroutines processing messages.
	// Default is 32 * number of CPUs.
	MaxWorkers int
	// Global limit of concurrently running workers across all servers.
	// Overrides MaxWorkers.
	WorkerLimit int
	// Maximum number of goroutines fetching messages.
	// Default is 4 * number of CPUs.
	MaxFetchers int

	// Compress data before sending to the queue.
	Compress bool

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

	// Number of tries/releases after which the message fails permanently
	// and is deleted.
	// Default is 64 retries.
	RetryLimit int
	// Minimum backoff time between retries.
	// Default is 30 seconds.
	MinBackoff time.Duration
	// Maximum backoff time between retries.
	// Default is 30 minutes.
	MaxBackoff time.Duration

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

func (opt *Options) Init() {
	if opt.inited {
		return
	}
	opt.inited = true

	if opt.GroupName == "" {
		opt.GroupName = opt.Name
	}

	if opt.WorkerLimit > 0 {
		opt.MaxWorkers = opt.WorkerLimit
	}
	if opt.MaxWorkers == 0 {
		opt.MaxWorkers = 32 * runtime.NumCPU()
	}
	if opt.MaxFetchers == 0 {
		opt.MaxFetchers = 4 * runtime.NumCPU()
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

	if opt.RetryLimit == 0 {
		opt.RetryLimit = 64
	}
	if opt.MinBackoff == 0 {
		opt.MinBackoff = 30 * time.Second
	}
	if opt.MaxBackoff == 0 {
		opt.MaxBackoff = 30 * time.Minute
	}

	if opt.Storage == nil {
		opt.Storage = redisStorage{opt.Redis}
	}

	if opt.RateLimit != rate.Inf && opt.RateLimiter == nil && opt.Redis != nil {
		limiter := redis_rate.NewLimiter(opt.Redis)
		limiter.Fallback = rate.NewLimiter(opt.RateLimit, 1)
		opt.RateLimiter = limiter
	}
}
