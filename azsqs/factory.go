package azsqs

import (
	"sync"

	"github.com/aws/aws-sdk-go/service/sqs"

	"github.com/vmihailenco/taskq"
)

type factory struct {
	sqs       *sqs.SQS
	accountID string

	queuesMu sync.RWMutex
	queues   []taskq.Queue
}

var _ taskq.Factory = (*factory)(nil)

func (f *factory) NewQueue(opt *taskq.QueueOptions) taskq.Queue {
	f.queuesMu.Lock()
	defer f.queuesMu.Unlock()

	q := NewQueue(f.sqs, f.accountID, opt)
	f.queues = append(f.queues, q)
	return q
}

func (f *factory) Queues() []taskq.Queue {
	f.queuesMu.RLock()
	defer f.queuesMu.RUnlock()
	return f.queues
}

func NewFactory(sqs *sqs.SQS, accountID string) taskq.Factory {
	return &factory{
		sqs:       sqs,
		accountID: accountID,
	}
}
