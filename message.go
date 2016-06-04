package queue

import (
	"fmt"
	"time"
)

type Message struct {
	Id    string
	Name  string
	Delay time.Duration
	Body  string
	Args  []interface{}

	ReservationId string
	ReservedCount int
}

func NewMessage(args ...interface{}) *Message {
	body, err := encodeArgs(args)
	if err != nil {
		panic(err)
	}
	return &Message{
		Body: body,
		Args: args,
	}
}

func (m *Message) String() string {
	return fmt.Sprintf("Message<Id=%q Name=%q>", m.Id, m.Name)
}
