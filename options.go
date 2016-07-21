package queue

import (
	"time"

	"golang.org/x/time/rate"
	"gopkg.in/redis.v4"
)

type Rediser interface {
	SetNX(string, interface{}, time.Duration) *redis.BoolCmd
	SAdd(key string, members ...interface{}) *redis.IntCmd
	SMembers(key string) *redis.StringSliceCmd
	Pipelined(func(pipe *redis.Pipeline) error) ([]redis.Cmder, error)
	Publish(channel, message string) *redis.IntCmd
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

	BufferSize int

	// Number of tries/releases after which the task fails permanently
	// and is deleted.
	RetryLimit int

	// Minimum time between retries.
	MinBackoff time.Duration

	RateLimit rate.Limit

	Redis Rediser
}
