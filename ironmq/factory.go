package ironmq

import (
	"sync"

	iron_config "github.com/iron-io/iron_go3/config"
	"github.com/iron-io/iron_go3/mq"

	"github.com/vmihailenco/taskq"
)

type factory struct {
	cfg *iron_config.Settings

	queuesMu sync.RWMutex
	queues   []taskq.Queue
}

var _ taskq.Factory = (*factory)(nil)

func (f *factory) NewQueue(opt *taskq.QueueOptions) taskq.Queue {
	f.queuesMu.Lock()
	defer f.queuesMu.Unlock()

	ironq := mq.ConfigNew(opt.Name, f.cfg)
	q := NewQueue(ironq, opt)
	f.queues = append(f.queues, q)
	return q
}

func (f *factory) Queues() []taskq.Queue {
	f.queuesMu.RLock()
	defer f.queuesMu.RUnlock()
	return f.queues
}

func NewFactory(cfg *iron_config.Settings) taskq.Factory {
	return &factory{
		cfg: cfg,
	}
}
