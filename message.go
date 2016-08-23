package queue

import (
	"errors"
	"fmt"
	"time"
)

var ErrDuplicate = errors.New("queue: message with such name already exists")

type Message struct {
	Id string

	// An unique name for the message.
	Name string

	// Delay specifies the duration the queue must wait
	// before executing the message.
	Delay time.Duration

	Args []interface{}
	Body string

	ReservationId string

	// The number of times the message has been reserved or released.
	ReservedCount int

	values map[string]interface{}
}

func NewMessage(args ...interface{}) *Message {
	return &Message{
		Args: args,
	}
}

func NewMessageOnce(delay time.Duration, args ...interface{}) *Message {
	msg := NewMessage(args...)
	msg.Name = fmt.Sprintf("%s:%d", fmt.Sprint(args), timeSlot(delay))
	msg.Delay = delay
	return msg
}

func WrapMessage(msg *Message) *Message {
	msg0 := NewMessage(msg)
	msg0.Name = msg.Name
	return msg0
}

func (m *Message) String() string {
	return fmt.Sprintf("Message<Id=%q Name=%q>", m.Id, m.Name)
}

func (m *Message) MarshalArgs() (string, error) {
	return encodeArgs(m.Args)
}

func (m *Message) SetValue(name string, value interface{}) {
	if m.values == nil {
		m.values = make(map[string]interface{})
	}
	m.values[name] = value
}

func (m *Message) Value(name string) interface{} {
	return m.values[name]
}

func timeSlot(resolution time.Duration) int64 {
	resolutionInSeconds := int64(resolution / time.Second)
	if resolutionInSeconds <= 0 {
		return 0
	}
	return time.Now().Unix() / resolutionInSeconds
}
