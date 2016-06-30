package memqueue

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"gopkg.in/queue.v1"
	"gopkg.in/queue.v1/processor"
)

const cachePrefix = "memqueue"

var (
	ErrDuplicate = errors.New("memqueue: message with such name already exists")
)

type Queue struct {
	opt *queue.Options

	noDelay bool

	p  *processor.Processor
	wg sync.WaitGroup
}

var _ queue.Queuer = (*Queue)(nil)

func NewQueue(opt *queue.Options) *Queue {
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
	msg.SetValue("sync", true)
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

func (q *Queue) AddAsync(msg *queue.Message) error {
	return q.addMessage(msg)
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

func (q *Queue) Close() error {
	return q.CloseTimeout(30 * time.Second)
}

func (q *Queue) CloseTimeout(timeout time.Duration) error {
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
		return ErrDuplicate
	}
	q.wg.Add(1)
	return q.enqueueMessage(msg)
}

func (q *Queue) enqueueMessage(msg *queue.Message) error {
	var delay time.Duration
	delay, msg.Delay = msg.Delay, 0
	msg.ReservedCount++

	var sync bool
	if q.noDelay {
		sync = true
		delay = 0
	} else {
		sync = msg.Value("sync") == true
	}

	if delay == 0 {
		if sync {
			return q.p.Process(msg)
		}
		return q.p.Add(msg)
	}

	if sync {
		time.Sleep(delay)
		return q.p.Process(msg)
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
	key := fmt.Sprintf("%s:%s:%s", cachePrefix, q.Name(), name)
	return !q.opt.Storage.Exists(key)
}

func (q *Queue) ReserveN(n int) ([]queue.Message, error) {
	select {}
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
	return nil
}
