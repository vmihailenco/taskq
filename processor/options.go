package processor

import (
	"time"

	"golang.org/x/time/rate"
)

type Options struct {
	Handler         interface{}
	FallbackHandler interface{}

	BufferSize   int
	WorkerNumber int
	Retries      int
	Backoff      time.Duration

	RateLimit rate.Limit
	Limiter   Limiter
}

func (opt *Options) init() {
	if opt.BufferSize == 0 {
		opt.BufferSize = 10
	}
	if opt.WorkerNumber == 0 {
		opt.WorkerNumber = 10
	}
	if opt.RateLimit == 0 {
		opt.RateLimit = rate.Inf
	}
	if opt.Retries == 0 {
		opt.Retries = 3
	}
	if opt.Backoff == 0 {
		opt.Backoff = time.Second
	}
}
