package azsqs

import (
	"github.com/aws/aws-sdk-go/service/sqs"

	"github.com/vmihailenco/taskq/v2"
	"github.com/vmihailenco/taskq/v2/internal/base"
)

type factory struct {
	base base.Factory

	sqs       *sqs.SQS
	accountID string
}

var _ taskq.Factory = (*factory)(nil)

func (f *factory) NewQueue(opt *taskq.QueueOptions) taskq.Queuer {
	q := NewQueue(f.sqs, f.accountID, opt)
	f.base.Add(q)
	return q
}

func (f *factory) Queues() []taskq.Queuer {
	return f.base.Queues()
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

func NewFactory(sqs *sqs.SQS, accountID string) taskq.Factory {
	return &factory{
		sqs:       sqs,
		accountID: accountID,
	}
}
