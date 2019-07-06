package ironmq

import (
	"context"

	iron_config "github.com/iron-io/iron_go3/config"
	"github.com/iron-io/iron_go3/mq"

	"github.com/vmihailenco/taskq/v2"
	"github.com/vmihailenco/taskq/v2/internal/base"
)

type factory struct {
	base base.Factory

	cfg *iron_config.Settings
}

var _ taskq.Factory = (*factory)(nil)

func (f *factory) RegisterQueue(opt *taskq.QueueOptions) taskq.Queue {
	ironq := mq.ConfigNew(opt.Name, f.cfg)
	q := NewQueue(ironq, opt)
	if err := f.base.Register(q); err != nil {
		panic(err)
	}
	return q
}

func (f *factory) Range(fn func(taskq.Queue) bool) {
	f.base.Range(fn)
}

func (f *factory) StartConsumers(ctx context.Context) error {
	return f.base.StartConsumers(ctx)
}

func (f *factory) StopConsumers() error {
	return f.base.StopConsumers()
}

func (f *factory) Close() error {
	return f.base.Close()
}

func NewFactory(cfg *iron_config.Settings) taskq.Factory {
	return &factory{
		cfg: cfg,
	}
}
