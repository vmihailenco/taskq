package msgutil

import (
	"encoding/binary"
	"fmt"

	"github.com/dgryski/go-farm"
	"github.com/vmihailenco/taskq/v3"
	"github.com/vmihailenco/taskq/v3/internal"
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
	ln := len(q.Name()) + len(msg.TaskName)
	data := make([]byte, 0, ln+len(msg.Name))
	data = append(data, q.Name()...)
	data = append(data, msg.TaskName...)
	data = append(data, msg.Name...)

	b := make([]byte, 3+8+8)
	copy(b, "tq:")

	// Hash message name.
	h := farm.Hash64(data[ln:])
	binary.BigEndian.PutUint64(b[3:11], h)

	// Hash queue name and use it as a seed.
	seed := farm.Hash64(data[:ln])

	// Hash everything using the seed.
	h = farm.Hash64WithSeed(data, seed)
	binary.BigEndian.PutUint64(b[11:19], h)

	return internal.BytesToString(b)
}
