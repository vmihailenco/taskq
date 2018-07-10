package msgutil

import (
	"fmt"

	"github.com/go-msgqueue/msgqueue"
)

func WrapMessage(msg *msgqueue.Message) *msgqueue.Message {
	msg0 := msgqueue.NewMessage(msg)
	msg0.Name = msg.Name
	return msg0
}

func UnwrapMessage(msg *msgqueue.Message) (*msgqueue.Message, error) {
	msg, ok := msg.Args[0].(*msgqueue.Message)
	if !ok {
		err := fmt.Errorf("UnwrapMessage: got %v, wanted *msgqueue.Message", msg.Args)
		return nil, err
	}
	return msg, nil
}

func UnwrapMessageHandler(h msgqueue.Handler) msgqueue.HandlerFunc {
	return msgqueue.HandlerFunc(func(msg *msgqueue.Message) error {
		msg, err := UnwrapMessage(msg)
		if err != nil {
			return err
		}
		return h.HandleMessage(msg)
	})
}
