package memqueue

import (
	"fmt"
	"sync"
	"time"

	"gopkg.in/queue.v1"
	"gopkg.in/queue.v1/processor"
)

const redisPrefix = "memqueue"

type Queue struct {
	opt *queue.Options

	noDelay bool

	p  *processor.Processor
	wg sync.WaitGroup
}

var _ processor.Queuer = (*Queue)(nil)

func NewQueue(opt *queue.Options) *Queue {
	opt.Init()
	q := Queue{
		opt: opt,
	}
	q.p = processor.Start(&q, opt)

	registerQueue(&q)
	return &q
}

func (q *Queue) Name() string {
	return q.opt.Name
}

func (q *Queue) String() string {
	return fmt.Sprintf("Memqueue<%s>", q.Name())
}

func (q *Queue) Options() *queue.Options {
	return q.opt
}

func (q *Queue) Processor() *processor.Processor {
	return q.p
}

func (q *Queue) SetNoDelay(f bool) {
	q.noDelay = f
}

func (q *Queue) Add(msg *queue.Message) error {
	return q.addMessage(msg)
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

func (q *Queue) Close() error {
	return q.CloseTimeout(30 * time.Second)
}

func (q *Queue) CloseTimeout(timeout time.Duration) error {
	defer q.p.Stop()
	defer unregisterQueue(q)

	done := make(chan struct{})
	go func() {
		q.wg.Wait()
		close(done)
	}()

	select {
	case <-time.After(timeout):
		return fmt.Errorf("workers did not stop after %s seconds", timeout)
	case <-done:
		return nil
	}
}

func (q *Queue) addMessage(msg *queue.Message) error {
	if !q.isUniqueName(msg.Name) {
		return queue.ErrDuplicate
	}
	q.wg.Add(1)
	return q.enqueueMessage(msg)
}

func (q *Queue) enqueueMessage(msg *queue.Message) error {
	var delay time.Duration
	delay, msg.Delay = msg.Delay, 0
	msg.ReservedCount++

	if q.noDelay {
		return q.p.Process(msg)
	}

	if delay == 0 {
		return q.p.Add(msg)
	}

	time.AfterFunc(delay, func() {
		q.p.Add(msg)
	})
	return nil
}

func (q *Queue) isUniqueName(name string) bool {
	if name == "" {
		return true
	}
	key := fmt.Sprintf("%s:%s:%s", redisPrefix, q.Name(), name)
	return q.opt.Redis.SetNX(key, nil, 24*time.Hour).Val()
}

func (q *Queue) ReserveN(n int) ([]queue.Message, error) {
	return nil, processor.ErrNotSupported
}

func (q *Queue) Release(msg *queue.Message, dur time.Duration) error {
	msg.Delay = dur
	return q.enqueueMessage(msg)
}

func (q *Queue) Delete(msg *queue.Message) error {
	q.wg.Done()
	return nil
}

func (q *Queue) DeleteBatch(msgs []*queue.Message) error {
	for _, msg := range msgs {
		if err := q.Delete(msg); err != nil {
			return err
		}
	}
	return nil
}

func (q *Queue) Purge() error {
	return q.p.Purge()
}
