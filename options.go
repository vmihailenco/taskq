package queue

import (
	"time"

	"golang.org/x/time/rate"
)

type Storager interface {
	Exists(msgName string) bool
}

type Limiter interface {
	AllowRate(name string, limit rate.Limit) (delay time.Duration, allow bool)
}

type Options struct {
	// Queue name.
	Name string

	// Checks if message name exists.
	Storage Storager

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

	Limiter Limiter

	RateLimit rate.Limit
}
