package processor

import (
	"time"

	"gopkg.in/queue.v1"
)

type Queuer interface {
	Name() string
	Add(msg *queue.Message) error
	AddAsync(msg *queue.Message) error
	ReserveN(n int) ([]queue.Message, error)
	Release(*queue.Message, time.Duration) error
	Delete(msg *queue.Message, reason error) error
	Purge() error
}
