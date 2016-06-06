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

type Memqueue struct {
	opt *Options

	p  *processor.Processor
	wg sync.WaitGroup
}

var _ processor.Queuer = (*Memqueue)(nil)

func NewMemqueue(opt *Options) *Memqueue {
	opt.init()

	q := Memqueue{
		opt: opt,
	}
	q.p = processor.Start(&q, &opt.Processor)

	registerQueue(&q)
	return &q
}

func (q *Memqueue) Name() string {
	return q.opt.Name
}

func (q *Memqueue) String() string {
	return fmt.Sprintf("Memqueue<%s>", q.Name())
}

func (q *Memqueue) Processor() *processor.Processor {
	return q.p
}

func (q *Memqueue) Add(msg *queue.Message) error {
	if !q.isUniqueName(msg.Name) {
		return ErrDuplicate
	}
	q.wg.Add(1)
	return q.p.Process(msg)
}

func (q *Memqueue) Call(args ...interface{}) error {
	msg := queue.NewMessage(args...)
	return q.Add(msg)
}

func (q *Memqueue) CallOnce(delay time.Duration, args ...interface{}) error {
	msg := queue.NewMessage(args...)
	msg.Name = fmt.Sprint(args)
	msg.Delay = delay
	return q.Add(msg)
}

func (q *Memqueue) AddAsync(msg *queue.Message) error {
	if !q.isUniqueName(msg.Name) {
		return ErrDuplicate
	}
	q.wg.Add(1)
	return q.addMessage(msg)
}

func (q *Memqueue) CallAsync(args ...interface{}) error {
	msg := queue.NewMessage(args...)
	return q.AddAsync(msg)
}

func (q *Memqueue) CallOnceAsync(delay time.Duration, args ...interface{}) error {
	msg := queue.NewMessage(args...)
	msg.Name = fmt.Sprint(args)
	msg.Delay = delay
	return q.AddAsync(msg)
}

func (q *Memqueue) Close() error {
	return q.CloseTimeout(30 * time.Second)
}

func (q *Memqueue) CloseTimeout(timeout time.Duration) error {
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

func (q *Memqueue) addMessage(msg *queue.Message) error {
	if q.opt.AlwaysSync {
		return q.p.Process(msg)
	}

	var delay time.Duration
	delay, msg.Delay = msg.Delay, 0
	msg.ReservedCount++

	if delay == 0 || q.opt.IgnoreDelay {
		return q.p.AddMessage(msg)
	}

	time.AfterFunc(delay, func() {
		q.p.AddMessage(msg)
	})
	return nil
}

func (q *Memqueue) isUniqueName(name string) bool {
	if name == "" {
		return true
	}
	key := fmt.Sprintf("%s:%s:%s", cachePrefix, q.Name(), name)
	return !q.opt.Cache.Exists(key)
}

func (q *Memqueue) ReserveN(n int) ([]queue.Message, error) {
	select {}
}

func (q *Memqueue) Release(msg *queue.Message, dur time.Duration) error {
	time.AfterFunc(dur, func() {
		msg.ReservedCount++
		q.p.AddMessage(msg)
	})
	return nil
}

func (q *Memqueue) Delete(msg *queue.Message, reason error) error {
	defer q.wg.Done()
	return nil
}

func (q *Memqueue) Purge() error {
	return nil
}
