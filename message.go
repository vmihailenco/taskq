package msgqueue

import (
	"errors"
	"fmt"
	"math/rand"
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
	Body    string
	bodyErr error

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
	// Some random delay to better distribute the load.
	m.Delay += time.Duration(rand.Intn(5)+1) * time.Second
}

func (m *Message) EncodeArgs() (string, error) {
	if m.bodyErr != nil {
		return "", m.bodyErr
	}
	if m.Body != "" {
		return m.Body, nil
	}
	m.Body, m.bodyErr = encodeArgs(m.Args)
	return m.Body, m.bodyErr
}

func timeSlot(resolution time.Duration) int64 {
	resolutionInSeconds := int64(resolution / time.Second)
	if resolutionInSeconds <= 0 {
		return 0
	}
	return time.Now().Unix() / resolutionInSeconds
}

func argsName(args []interface{}) string {
	b, _ := msgpack.Marshal(args...)
	return string(b)
}
