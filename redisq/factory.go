package redisq

import (
	"sync"

	"github.com/vmihailenco/taskq"
)

type factory struct {
	queuesMu sync.RWMutex
	queues   []taskq.Queue
}

var _ taskq.Factory = (*factory)(nil)

func (f *factory) NewQueue(opt *taskq.QueueOptions) taskq.Queue {
	f.queuesMu.Lock()
	defer f.queuesMu.Unlock()

	q := NewQueue(opt)
	f.queues = append(f.queues, q)
	return q
}

func (f *factory) Queues() []taskq.Queue {
	f.queuesMu.RLock()
	defer f.queuesMu.RUnlock()
	return f.queues
}

func NewFactory() taskq.Factory {
	return &factory{}
}
