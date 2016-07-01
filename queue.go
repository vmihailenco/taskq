package queue

import (
	"time"

	"golang.org/x/time/rate"
)

type Storager interface {
	Exists(key string) bool
}

type Limiter interface {
	AllowRate(name string, limit rate.Limit) (delay time.Duration, allow bool)
}
