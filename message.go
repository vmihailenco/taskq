package queue

import (
	"errors"
	"fmt"
	"time"
)

var ErrDuplicate = errors.New("queue: message with such name already exists")

type Message struct {
	Id    string
	Name  string
	Delay time.Duration

	Args []interface{}
	Body string

	ReservationId string
	ReservedCount int

	values map[string]interface{}
}

func NewMessage(args ...interface{}) *Message {
	return &Message{
		Args: args,
	}
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
