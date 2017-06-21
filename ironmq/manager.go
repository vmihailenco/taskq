package ironmq

import (
	"github.com/go-msgqueue/msgqueue"
	iron_config "github.com/iron-io/iron_go3/config"
	"github.com/iron-io/iron_go3/mq"
)

type Manager struct {
	conf *iron_config.Settings
}

func (m *Manager) NewQueue(opt *msgqueue.Options) *Queue {
	mqueue := mq.ConfigNew(opt.Name, m.conf)
	return NewQueue(mqueue, opt)
}

func (m *Manager) Queues() []*Queue {
	return Queues()
}

func NewIronManager(host, projectId, token string) *Manager {
	conf := &iron_config.Settings{
		Host:      host,
		ProjectId: projectId,
		Token:     token,
	}

	man := &Manager{
		conf: conf,
	}

	return man
}
