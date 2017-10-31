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
	Len() (int, error)
	ReserveN(n int) ([]*Message, error)
	Release(*Message) error
	Delete(msg *Message) error
	Purge() error
	Close() error
	CloseTimeout(timeout time.Duration) error
}

// Manager is an interface that abstracts creation of new queues.
// It is implemented in subpackages memqueue, azsqs, and ironmq.
type Manager interface {
	NewQueue(*Options) Queue
	Queues() []Queue
}
