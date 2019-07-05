package redisq

import (
	"github.com/vmihailenco/taskq/v2"
	"github.com/vmihailenco/taskq/v2/internal/base"
)

type factory struct {
	base base.Factory
}

var _ taskq.Factory = (*factory)(nil)

func NewFactory() taskq.Factory {
	return &factory{}
}

func (f *factory) RegisterQueue(opt *taskq.QueueOptions) taskq.Queue {
	q := NewQueue(opt)
	if err := f.base.Register(q); err != nil {
		panic(err)
	}
	return q
}

func (f *factory) Range(fn func(taskq.Queue) bool) {
	f.base.Range(fn)
}

func (f *factory) StartConsumers() error {
	return f.base.StartConsumers()
}

func (f *factory) StopConsumers() error {
	return f.base.StopConsumers()
}

func (f *factory) Close() error {
	return f.base.Close()
}
