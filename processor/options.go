package processor

import (
	"runtime"
	"time"

	"golang.org/x/time/rate"
)

type Options struct {
	Handler         interface{}
	FallbackHandler interface{}

	Workers    int
	BufferSize int
	Retries    int
	Backoff    time.Duration

	RateLimit rate.Limit
	Limiter   Limiter
}

func (opt *Options) init() {
	if opt.Workers == 0 {
		opt.Workers = runtime.NumCPU() * 10
	}
	if opt.BufferSize == 0 {
		opt.BufferSize = 10
	}
	if opt.RateLimit == 0 {
		opt.RateLimit = rate.Inf
	}
	if opt.Retries == 0 {
		opt.Retries = 10
	}
	if opt.Backoff == 0 {
		opt.Backoff = 3 * time.Second
	}
}
