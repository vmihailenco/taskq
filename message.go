package msgqueue

import (
	"errors"
	"fmt"
	"hash/fnv"
	"time"

	"github.com/go-msgqueue/msgqueue/internal"
	"github.com/vmihailenco/msgpack"
)

// ErrDuplicate is returned when adding duplicate message to the queue.
var ErrDuplicate = errors.New("queue: message with such name already exists")

// Message is used to create and retrieve messages from a queue.
type Message struct {
	// SQS/IronMQ message id.
	Id string

	// Optional name for the message. Messages with the same name
	// are processed only once.
	Name string

	// Delay specifies the duration the queue must wait
	// before executing the message.
	Delay time.Duration

	// Function args passed to the handler.
	Args []interface{}

	// Text representation of the Args.
	Body string

	// SQS/IronMQ reservation id that is used to release/delete the message..
	ReservationId string

	// The number of times the message has been reserved or released.
	ReservedCount int

	Err error
}

func NewMessage(args ...interface{}) *Message {
	return &Message{
		Args: args,
	}
}

func (m *Message) String() string {
	return fmt.Sprintf("Message<Id=%q Name=%q ReservedCount=%d>",
		m.Id, m.Name, m.ReservedCount)
}

// SetDelayName sets delay and generates message name from the args.
func (m *Message) SetDelayName(delay time.Duration, args ...interface{}) {
	h := hashArgs(append(args, delay, timeSlot(delay)))
	m.Name = string(h)
	m.Delay = delay
}

func (m *Message) EncodeBody(compress bool) (string, error) {
	if m.Body != "" {
		return m.Body, nil
	}

	s, err := internal.EncodeArgs(m.Args, compress)
	if err != nil {
		return "", err
	}

	m.Body = s
	return s, nil
}

func timeSlot(period time.Duration) int64 {
	if period <= 0 {
		return 0
	}
	return time.Now().UnixNano() / int64(period)
}

func hashArgs(args []interface{}) []byte {
	b, _ := msgpack.Marshal(args...)
	if len(b) <= 128 {
		return b
	}

	h := fnv.New128a()
	_, _ = h.Write(b)
	return h.Sum(nil)
}
