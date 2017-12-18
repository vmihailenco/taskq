package msgqueue

import (
	"errors"
	"fmt"
	"time"

	"github.com/vmihailenco/msgpack"
)

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
	return fmt.Sprintf(
		"Message<Id=%q Name=%q ReservedCount=%d>",
		m.Id, m.Name, m.ReservedCount,
	)
}

// SetDelayName sets delay and generates message name from the args.
func (m *Message) SetDelayName(delay time.Duration, args ...interface{}) {
	m.Name = argsName(append(args, timeSlot(delay)))
	m.Delay = delay
}

func timeSlot(resolution time.Duration) int64 {
	if resolution <= 0 {
		return 0
	}
	return time.Now().UnixNano() / int64(resolution)
}

func argsName(args []interface{}) string {
	b, _ := msgpack.Marshal(args...)
	return string(b)
}
