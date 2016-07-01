package queue

import (
	"time"

	"golang.org/x/time/rate"

	"gopkg.in/queue.v1/processor"
)

type Storager interface {
	Exists(key string) bool
}

type Limiter interface {
	AllowRate(name string, limit rate.Limit) (delay time.Duration, allow bool)
}

type Queuer interface {
	Name() string
	Processor() *processor.Processor
	Add(msg *Message) error
	Call(args ...interface{}) error
	CallOnce(dur time.Duration, args ...interface{}) error
	AddAsync(msg *Message) error
	CallAsync(args ...interface{}) error
	CallOnceAsync(dur time.Duration, args ...interface{}) error
	ReserveN(n int) ([]Message, error)
	Release(*Message, time.Duration) error
	Delete(msg *Message) error
	DeleteBatch(msg []*Message) error
	Purge() error
}
