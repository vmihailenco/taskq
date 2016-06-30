package ironmq

import (
	"fmt"
	"strings"
	"time"

	"github.com/iron-io/iron_go3/api"
	"github.com/iron-io/iron_go3/mq"

	"gopkg.in/queue.v1"
	"gopkg.in/queue.v1/memqueue"
	"gopkg.in/queue.v1/processor"
)

type Queue struct {
	q        mq.Queue
	opt      *processor.Options
	memqueue *memqueue.Memqueue
}

func NewQueue(mqueue mq.Queue, opt *Options) *Queue {
	opt.Name = mqueue.Name
	q := Queue{
		q:   mqueue,
		opt: &opt.Processor,
	}

	popt := opt.Processor
	if !opt.Offline {
		popt.Retries = 3
		popt.Backoff = time.Second
		popt.FallbackHandler = popt.Handler
		popt.Handler = queue.HandlerFunc(q.add)
	}
	memopt := memqueue.Options{
		Name:    opt.Name,
		Storage: opt.Storage,

		Processor:  popt,
		AlwaysSync: opt.AlwaysSync,
	}
	q.memqueue = memqueue.NewMemqueue(&memopt)

	registerQueue(&q)
	return &q
}

func (q *Queue) Name() string {
	return q.q.Name
}

func (q *Queue) String() string {
	return fmt.Sprintf("Queue<%s>", q.Name())
}

func (q *Queue) Processor() *processor.Processor {
	return processor.New(q, q.opt)
}

func (q *Queue) createQueue() error {
	_, err := mq.ConfigCreateQueue(mq.QueueInfo{Name: q.q.Name}, &q.q.Settings)
	return err
}

func (q *Queue) add(msg *queue.Message) error {
	id, err := q.q.PushMessage(mq.Message{
		Body:  msg.Body,
		Delay: int64(msg.Delay / time.Second),
	})
	if err != nil {
		return err
	}
	msg.Id = id
	return nil
}

func (q *Queue) Add(msg *queue.Message) error {
	msg.Wrapped = true
	return q.memqueue.Add(msg)
}

func (q *Queue) Call(args ...interface{}) error {
	msg := queue.NewMessage(args...)
	return q.Add(msg)
}

func (q *Queue) CallOnce(delay time.Duration, args ...interface{}) error {
	msg := queue.NewMessage(args...)
	msg.Name = fmt.Sprint(args)
	msg.Delay = delay
	return q.Add(msg)
}

func (q *Queue) AddAsync(msg *queue.Message) error {
	msg.Wrapped = true
	return q.memqueue.AddAsync(msg)
}

func (q *Queue) CallAsync(args ...interface{}) error {
	msg := queue.NewMessage(args...)
	return q.AddAsync(msg)
}

func (q *Queue) CallOnceAsync(delay time.Duration, args ...interface{}) error {
	msg := queue.NewMessage(args...)
	msg.Name = fmt.Sprint(args)
	msg.Delay = delay
	return q.AddAsync(msg)
}

func (q *Queue) ReserveN(n int) ([]queue.Message, error) {
	if n > 100 {
		n = 100
	}
	mqMsgs, err := q.q.LongPoll(n, 300, 1, false)
	if err != nil {
		if v, ok := err.(api.HTTPResponseError); ok && v.StatusCode() == 404 {
			if strings.Contains(v.Error(), "Message not found") {
				return nil, nil
			}
			if strings.Contains(v.Error(), "Queue not found") {
				_ = q.createQueue()
			}
		}
		return nil, err
	}

	msgs := make([]queue.Message, len(mqMsgs))
	for i, mqMsg := range mqMsgs {
		msgs[i] = queue.Message{
			Id:   mqMsg.Id,
			Body: mqMsg.Body,

			ReservationId: mqMsg.ReservationId,
			ReservedCount: mqMsg.ReservedCount,
		}
	}
	return msgs, nil
}

func (q *Queue) Release(msg *queue.Message, delay time.Duration) error {
	return retry(func() error {
		return q.q.ReleaseMessage(msg.Id, msg.ReservationId, int64(delay/time.Second))
	})
}

func (q *Queue) Delete(msg *queue.Message) error {
	err := retry(func() error {
		return q.q.DeleteMessage(msg.Id, msg.ReservationId)
	})
	if err == nil {
		return nil
	}
	if v, ok := err.(api.HTTPResponseError); ok && v.StatusCode() == 404 {
		return nil
	}
	return err
}

func (q *Queue) DeleteBatch(msgs []*queue.Message) error {
	mqMsgs := make([]mq.Message, len(msgs))
	for i, msg := range msgs {
		mqMsgs[i] = mq.Message{
			Id:            msg.Id,
			ReservationId: msg.ReservationId,
		}
	}
	return retry(func() error {
		return q.q.DeleteReservedMessages(mqMsgs)
	})
}

func (q *Queue) Purge() error {
	return q.q.Clear()
}

func (q *Queue) Close() error {
	return q.memqueue.Close()
}

func retry(fn func() error) error {
	var err error
	for i := 0; i < 3; i++ {
		err = fn()
		if err == nil {
			return nil
		}
		if v, ok := err.(api.HTTPResponseError); ok && v.StatusCode() >= 500 {
			continue
		}
		break
	}
	return err
}
