package queue

import (
	"time"

	"golang.org/x/time/rate"
)

type Options struct {
	Name    string
	Storage Storager

	Handler         interface{}
	FallbackHandler interface{}

	Workers    int
	Scavengers int
	BufferSize int
	Retries    int
	Backoff    time.Duration

	RateLimit rate.Limit
	Limiter   Limiter
}
