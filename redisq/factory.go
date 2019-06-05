package redisq

import (
	"github.com/vmihailenco/taskq"
	"github.com/vmihailenco/taskq/internal/base"
)

type factory struct {
	base base.Factory
}

var _ taskq.Factory = (*factory)(nil)

func (f *factory) NewQueue(opt *taskq.QueueOptions) taskq.Queue {
	q := NewQueue(opt)
	f.base.Add(q)
	return q
}

func (f *factory) Queues() []taskq.Queue {
	return f.base.Queues()
}

func (f *factory) StartConsumers() error {
	return f.base.StartConsumers()
}

func (f *factory) CloseConsumers() error {
	return f.base.CloseConsumers()
}

func (f *factory) Close() error {
	return f.base.Close()
}

func NewFactory() taskq.Factory {
	return &factory{}
}
