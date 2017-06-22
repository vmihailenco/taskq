package msgqueue

import (
	"log"
	"os"
	"time"

	"github.com/go-msgqueue/msgqueue/internal"
)

func init() {
	SetLogger(log.New(os.Stderr, "msgqueue: ", log.LstdFlags))
}

func SetLogger(logger *log.Logger) {
	internal.Logger = logger
}

type Queue interface {
	Name() string
	Processor() *Processor
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
