package internal

import "gopkg.in/queue.v1"

func WrapMessage(msg *queue.Message) *queue.Message {
	msg0 := queue.NewMessage(msg)
	msg0.Name = msg.Name
	return msg0
}

func MessageUnwrapperHandler(fn interface{}) queue.HandlerFunc {
	h := queue.NewHandler(fn)
	return queue.HandlerFunc(func(msg *queue.Message) error {
		msg = msg.Args[0].(*queue.Message)
		return h.HandleMessage(msg)
	})
}
