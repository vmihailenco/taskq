package msgutil

import (
	"fmt"

	"github.com/vmihailenco/taskq/v2"
)

func WrapMessage(msg *taskq.Message) *taskq.Message {
	msg0 := taskq.NewMessage(msg.Ctx, msg)
	msg0.Name = msg.Name
	return msg0
}

func UnwrapMessage(msg *taskq.Message) (*taskq.Message, error) {
	if len(msg.Args) != 1 {
		err := fmt.Errorf("UnwrapMessage: got %d args, wanted 1", len(msg.Args))
		return nil, err
	}

	msg, ok := msg.Args[0].(*taskq.Message)
	if !ok {
		err := fmt.Errorf("UnwrapMessage: got %v, wanted *taskq.Message", msg.Args)
		return nil, err
	}
	return msg, nil
}

func UnwrapMessageHandler(fn interface{}) taskq.HandlerFunc {
	if fn == nil {
		return nil
	}
	h := fn.(func(*taskq.Message) error)
	return taskq.HandlerFunc(func(msg *taskq.Message) error {
		msg, err := UnwrapMessage(msg)
		if err != nil {
			return err
		}
		return h(msg)
	})
}

func FullMessageName(q taskq.Queue, msg *taskq.Message) string {
	return "taskq:" + q.Name() + ":" + msg.TaskName + ":" + msg.Name
}
