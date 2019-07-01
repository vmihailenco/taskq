package memqueue

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/vmihailenco/taskq"
	"github.com/vmihailenco/taskq/internal"
	"github.com/vmihailenco/taskq/internal/base"
)

type Queue struct {
	base base.Queue

	opt *taskq.QueueOptions

	sync    bool
	noDelay bool

	wg       sync.WaitGroup
	consumer *taskq.Consumer

	_closed int32
}

var _ taskq.Queue = (*Queue)(nil)

func NewQueue(opt *taskq.QueueOptions) *Queue {
	opt.Init()

	q := &Queue{
		opt: opt,
	}
	q.consumer = taskq.NewConsumer(q)
	if err := q.consumer.Start(); err != nil {
		panic(err)
	}

	return q
}

func (q *Queue) Name() string {
	return q.opt.Name
}

func (q *Queue) String() string {
	return fmt.Sprintf("Memqueue<Name=%s>", q.Name())
}

func (q *Queue) Options() *taskq.QueueOptions {
	return q.opt
}

func (q *Queue) HandleMessage(msg *taskq.Message) error {
	return q.base.HandleMessage(msg)
}

func (q *Queue) NewTask(opt *taskq.TaskOptions) *taskq.Task {
	return q.base.NewTask(q, opt)
}

func (q *Queue) GetTask(name string) *taskq.Task {
	return q.base.GetTask(name)
}

func (q *Queue) RemoveTask(name string) {
	q.base.RemoveTask(name)
}

func (q *Queue) Consumer() *taskq.Consumer {
	return q.consumer
}

func (q *Queue) SetSync(sync bool) {
	q.sync = sync
}

func (q *Queue) SetNoDelay(noDelay bool) {
	q.noDelay = noDelay
}

// Close is like CloseTimeout with 30 seconds timeout.
func (q *Queue) Close() error {
	return q.CloseTimeout(30 * time.Second)
}

// CloseTimeout closes the queue waiting for pending messages to be processed.
func (q *Queue) CloseTimeout(timeout time.Duration) error {
	if !atomic.CompareAndSwapInt32(&q._closed, 0, 1) {
		return fmt.Errorf("taskq: %s is already closed", q)
	}

	done := make(chan struct{}, 1)
	go func() {
		q.wg.Wait()
		done <- struct{}{}
	}()

	select {
	case <-done:
	case <-time.After(timeout):
		return fmt.Errorf("taskq: %s: messages are not processed after %s", q.consumer, timeout)
	}

	_ = q.consumer.StopTimeout(timeout)

	return nil
}

func (q *Queue) Len() (int, error) {
	return q.consumer.Len(), nil
}

// Add adds message to the queue.
func (q *Queue) Add(msg *taskq.Message) error {
	if q.closed() {
		return fmt.Errorf("taskq: %s is closed", q)
	}
	if msg.TaskName == "" {
		return internal.ErrTaskNameRequired
	}
	q.wg.Add(1)
	return q.enqueueMessage(msg)
}

func (q *Queue) enqueueMessage(msg *taskq.Message) error {
	if (q.noDelay || q.sync) && msg.Delay > 0 {
		msg.Delay = 0
	}
	msg.ReservedCount++

	if q.sync {
		return q.consumer.Process(msg)
	}
	return q.consumer.Add(msg)
}

func (q *Queue) ReserveN(n int, waitTimeout time.Duration) ([]taskq.Message, error) {
	return nil, internal.ErrNotSupported
}

func (q *Queue) Release(msg *taskq.Message) error {
	return q.enqueueMessage(msg)
}

func (q *Queue) Delete(msg *taskq.Message) error {
	q.wg.Done()
	return nil
}

func (q *Queue) DeleteBatch(msgs []*taskq.Message) error {
	if len(msgs) == 0 {
		return errors.New("taskq: no messages to delete")
	}
	for _, msg := range msgs {
		if err := q.Delete(msg); err != nil {
			return err
		}
	}
	return nil
}

func (q *Queue) Purge() error {
	return q.consumer.Purge()
}

func (q *Queue) closed() bool {
	return atomic.LoadInt32(&q._closed) == 1
}
