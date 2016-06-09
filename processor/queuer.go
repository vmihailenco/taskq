package processor

import (
	"time"

	"gopkg.in/queue.v1"
)

type Queuer interface {
	Name() string
	Add(msg *queue.Message) error
	Call(args ...interface{}) error
	AddAsync(msg *queue.Message) error
	CallAsync(args ...interface{}) error
	ReserveN(n int) ([]queue.Message, error)
	Release(*queue.Message, time.Duration) error
	Delete(msg *queue.Message) error
	DeleteBatch(msg []*queue.Message) error
	Purge() error
}
