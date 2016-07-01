package internal

import "gopkg.in/queue.v1"

func MessageUnwrapperHandler(fn interface{}) queue.HandlerFunc {
	h := queue.NewHandler(fn)
	return queue.HandlerFunc(func(msg *queue.Message) error {
		msg = msg.Args[0].(*queue.Message)
		return h.HandleMessage(msg)
	})
}
