package internal

import "gopkg.in/msgqueue.v1"

func WrapMessage(msg *msgqueue.Message) *msgqueue.Message {
	msg0 := msgqueue.NewMessage(msg)
	msg0.Name = msg.Name
	return msg0
}

func MessageUnwrapperHandler(fn interface{}) msgqueue.HandlerFunc {
	h := msgqueue.NewHandler(fn)
	return msgqueue.HandlerFunc(func(msg *msgqueue.Message) error {
		msg = msg.Args[0].(*msgqueue.Message)
		return h.HandleMessage(msg)
	})
}
