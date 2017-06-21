package memqueue

import "github.com/go-msgqueue/msgqueue"

type Manager struct{}

func (m *Manager) NewQueue(opt *msgqueue.Options) *Queue {
	return NewQueue(opt)
}

func (m *Manager) Queues() []*Queue {
	return Queues()
}

func NewMemManager() *Manager {
	return &Manager{}
}
