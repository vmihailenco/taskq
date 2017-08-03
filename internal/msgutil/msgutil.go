package msgutil

import "github.com/go-msgqueue/msgqueue"

func WrapMessage(msg *msgqueue.Message) *msgqueue.Message {
	msg0 := msgqueue.NewMessage(msg)
	msg0.Name = msg.Name
	return msg0
}

func UnwrapMessageHandler(fn func(*msgqueue.Message) error) msgqueue.HandlerFunc {
	h := msgqueue.HandlerFunc(fn)
	return msgqueue.HandlerFunc(func(msg *msgqueue.Message) error {
		msg = msg.Args[0].(*msgqueue.Message)
		return h.HandleMessage(msg)
	})
}
