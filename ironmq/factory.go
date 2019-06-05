package ironmq

import (
	iron_config "github.com/iron-io/iron_go3/config"
	"github.com/iron-io/iron_go3/mq"

	"github.com/vmihailenco/taskq"
	"github.com/vmihailenco/taskq/internal/base"
)

type factory struct {
	base base.Factory

	cfg *iron_config.Settings
}

var _ taskq.Factory = (*factory)(nil)

func (f *factory) NewQueue(opt *taskq.QueueOptions) taskq.Queue {
	ironq := mq.ConfigNew(opt.Name, f.cfg)
	q := NewQueue(ironq, opt)
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

func NewFactory(cfg *iron_config.Settings) taskq.Factory {
	return &factory{
		cfg: cfg,
	}
}
