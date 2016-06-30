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

type Queuer interface {
	Name() string
	Add(msg *Message) error
	Call(args ...interface{}) error
	AddAsync(msg *Message) error
	CallAsync(args ...interface{}) error
	ReserveN(n int) ([]Message, error)
	Release(*Message, time.Duration) error
	Delete(msg *Message) error
	DeleteBatch(msg []*Message) error
	Purge() error
}
