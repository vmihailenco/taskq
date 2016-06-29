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
	msg.SetValue("sync", true)
	return q.addMessage(msg, true)
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
	return q.addMessage(msg, false)
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

func (q *Memqueue) addMessage(msg *queue.Message, sync bool) error {
	if !q.isUniqueName(msg.Name) {
		return ErrDuplicate
	}
	q.wg.Add(1)
	return q.enqueueMessage(msg, sync)
}

func (q *Memqueue) enqueueMessage(msg *queue.Message, sync bool) error {
	msg.ReservedCount++

	if q.opt.AlwaysSync {
		return q.p.Process(msg)
	}

	if q.opt.Processor.IgnoreMessageDelay || msg.Delay == 0 {
		if sync {
			return q.p.Process(msg)
		}
		return q.p.Add(msg)
	}

	var delay time.Duration
	delay, msg.Delay = msg.Delay, 0

	time.AfterFunc(delay, func() {
		q.p.Add(msg)
	})
	return nil
}

func (q *Memqueue) isUniqueName(name string) bool {
	if name == "" {
		return true
	}
	key := fmt.Sprintf("%s:%s:%s", cachePrefix, q.Name(), name)
	return !q.opt.Storage.Exists(key)
}

func (q *Memqueue) ReserveN(n int) ([]queue.Message, error) {
	select {}
}

func (q *Memqueue) Release(msg *queue.Message, dur time.Duration) error {
	msg.Delay = 0

	sync := msg.Value("sync")
	if sync != nil {
		time.Sleep(dur)
		err := q.enqueueMessage(msg, true)
		msg.SetValue("err", err)
		return nil
	}

	time.AfterFunc(dur, func() {
		q.enqueueMessage(msg, false)
	})
	return nil
}

func (q *Memqueue) Delete(msg *queue.Message) error {
	q.wg.Done()
	return nil
}

func (q *Memqueue) DeleteBatch(msgs []*queue.Message) error {
	for _, msg := range msgs {
		if err := q.Delete(msg); err != nil {
			return err
		}
	}
	return nil
}

func (q *Memqueue) Purge() error {
	return nil
}
