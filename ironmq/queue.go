package ironmq

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/iron-io/iron_go3/api"
	iron_config "github.com/iron-io/iron_go3/config"
	"github.com/iron-io/iron_go3/mq"

	"github.com/go-msgqueue/msgqueue"
	"github.com/go-msgqueue/msgqueue/internal/msgutil"
	"github.com/go-msgqueue/msgqueue/memqueue"
)

type manager struct {
	cfg *iron_config.Settings
}

func (m *manager) NewQueue(opt *msgqueue.Options) msgqueue.Queue {
	q := mq.ConfigNew(opt.Name, m.cfg)
	return NewQueue(q, opt)
}

func (manager) Queues() []msgqueue.Queue {
	var queues []msgqueue.Queue
	for _, q := range Queues() {
		queues = append(queues, q)
	}
	return queues
}

func NewManager(cfg *iron_config.Settings) msgqueue.Manager {
	return &manager{
		cfg: cfg,
	}
}

type Queue struct {
	q        mq.Queue
	opt      *msgqueue.Options
	memqueue *memqueue.Queue

	p *msgqueue.Processor
}

var _ msgqueue.Queue = (*Queue)(nil)

func NewQueue(mqueue mq.Queue, opt *msgqueue.Options) *Queue {
	if opt.Name == "" {
		opt.Name = mqueue.Name
	}
	opt.Init()

	q := Queue{
		q:   mqueue,
		opt: opt,
	}

	memopt := msgqueue.Options{
		Name:      opt.Name,
		GroupName: opt.GroupName,

		RetryLimit: 3,
		MinBackoff: time.Second,
		Handler:    msgqueue.HandlerFunc(q.add),

		Redis: opt.Redis,
	}
	if opt.Handler != nil {
		memopt.FallbackHandler = msgutil.MessageUnwrapperHandler(opt.Handler)
	}
	q.memqueue = memqueue.NewQueue(&memopt)

	registerQueue(&q)
	return &q
}

func (q *Queue) Name() string {
	return q.q.Name
}

func (q *Queue) String() string {
	return fmt.Sprintf("Queue<Name=%s>", q.Name())
}

func (q *Queue) Options() *msgqueue.Options {
	return q.opt
}

func (q *Queue) Processor() *msgqueue.Processor {
	if q.p == nil {
		q.p = msgqueue.NewProcessor(q, q.opt)
	}
	return q.p
}

func (q *Queue) createQueue() error {
	_, err := mq.ConfigCreateQueue(mq.QueueInfo{Name: q.q.Name}, &q.q.Settings)
	return err
}

func (q *Queue) add(msg *msgqueue.Message) error {
	msg = msg.Args[0].(*msgqueue.Message)

	body, err := msg.MarshalArgs()
	if err != nil {
		return err
	}

	id, err := q.q.PushMessage(mq.Message{
		Body:  body,
		Delay: int64(msg.Delay / time.Second),
	})
	if err != nil {
		return err
	}

	msg.Id = id
	return nil
}

// Add adds message to the queue.
func (q *Queue) Add(msg *msgqueue.Message) error {
	return q.memqueue.Add(msgutil.WrapMessage(msg))
}

// Call creates a message using the args and adds it to the queue.
func (q *Queue) Call(args ...interface{}) error {
	msg := msgqueue.NewMessage(args...)
	return q.Add(msg)
}

// CallOnce works like Call, but it adds message with same args
// only once in a period.
func (q *Queue) CallOnce(period time.Duration, args ...interface{}) error {
	msg := msgqueue.NewMessage(args...)
	msg.SetDelayName(period, args...)
	return q.Add(msg)
}

func (q *Queue) ReserveN(n int) ([]*msgqueue.Message, error) {
	if n > 100 {
		n = 100
	}

	reservationSecs := int(q.opt.ReservationTimeout / time.Second)
	waitSecs := int(q.opt.WaitTimeout / time.Second)

	mqMsgs, err := q.q.LongPoll(n, reservationSecs, waitSecs, false)
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

	msgs := make([]*msgqueue.Message, len(mqMsgs))
	for i, mqMsg := range mqMsgs {
		msgs[i] = &msgqueue.Message{
			Id:   mqMsg.Id,
			Body: mqMsg.Body,

			ReservationId: mqMsg.ReservationId,
			ReservedCount: mqMsg.ReservedCount,
		}
	}
	return msgs, nil
}

func (q *Queue) Release(msg *msgqueue.Message, delay time.Duration) error {
	return retry(func() error {
		return q.q.ReleaseMessage(msg.Id, msg.ReservationId, int64(delay/time.Second))
	})
}

func (q *Queue) Delete(msg *msgqueue.Message) error {
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

func (q *Queue) DeleteBatch(msgs []*msgqueue.Message) error {
	if len(msgs) == 0 {
		return errors.New("msgqueue: no messages to delete")
	}

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

// Close is CloseTimeout with 30 seconds timeout.
func (q *Queue) Close() error {
	return q.CloseTimeout(30 * time.Second)
}

// Close closes the queue waiting for pending messages to be processed.
func (q *Queue) CloseTimeout(timeout time.Duration) error {
	var firstErr error
	if err := q.memqueue.CloseTimeout(timeout); err != nil && firstErr == nil {
		firstErr = err
	}
	if q.p != nil {
		if err := q.p.StopTimeout(timeout); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
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
