package msgqueue

import (
	"time"
)

type Processor interface {
	Process(msg *Message) error
	ProcessOne() error
	ProcessAll() error

	Add(msg *Message) error
	Start() error
	Stop() error
	StopTimeout(timeout time.Duration) error
}

type Queue interface {
	Name() string
	Processor() Processor
	Add(msg *Message) error
	Call(args ...interface{}) error
	CallOnce(dur time.Duration, args ...interface{}) error
	ReserveN(n int) ([]Message, error)
	Release(*Message, time.Duration) error
	Delete(msg *Message) error
	DeleteBatch(msg []*Message) error
	Purge() error
	Close() error
	CloseTimeout(timeout time.Duration) error
}

type Manager interface {
	NewQueue(*Options) Queue
	Queues() []Queue
}
