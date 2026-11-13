package azsqs

import (
	"context"

	"github.com/aws/aws-sdk-go/service/sqs"

	"github.com/vmihailenco/taskq/v5"
	"github.com/vmihailenco/taskq/v5/internal/base"
)

type factory struct {
	base base.Factory

	sqs       *sqs.SQS
	accountID string
}

var _ taskq.Factory = (*factory)(nil)

func NewFactory(client *sqs.SQS, accountID string) taskq.Factory {
	return &factory{
		sqs:       client,
		accountID: accountID,
	}
}

func (f *factory) RegisterQueue(opt *taskq.QueueOptions) taskq.Queue {
	q := NewQueue(f.sqs, f.accountID, opt)
	if err := f.base.Register(q); err != nil {
		panic(err)
	}
	return q
}

func (f *factory) Range(fn func(queue taskq.Queue) bool) {
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
