package memqueue

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/vmihailenco/taskq/v3"
	"github.com/vmihailenco/taskq/v3/internal"
	"github.com/vmihailenco/taskq/v3/internal/msgutil"
)

type scheduler struct {
	timerLock sync.Mutex
	timerMap  map[*taskq.Message]*time.Timer
}

func (q *scheduler) Schedule(msg *taskq.Message, fn func()) {
	q.timerLock.Lock()
	defer q.timerLock.Unlock()

	timer := time.AfterFunc(msg.Delay, func() {
		// Remove our entry from the map
		q.timerLock.Lock()
		delete(q.timerMap, msg)
		q.timerLock.Unlock()

		fn()
	})

	if q.timerMap == nil {
		q.timerMap = make(map[*taskq.Message]*time.Timer)
	}
	q.timerMap[msg] = timer
}

func (q *scheduler) Remove(msg *taskq.Message) {
	q.timerLock.Lock()
	defer q.timerLock.Unlock()

	timer, ok := q.timerMap[msg]
	if ok {
		timer.Stop()
		delete(q.timerMap, msg)
	}
}

func (q *scheduler) Purge() int {
	q.timerLock.Lock()
	defer q.timerLock.Unlock()

	// Stop all delayed items
	for _, timer := range q.timerMap {
		timer.Stop()
	}

	n := len(q.timerMap)
	q.timerMap = nil

	return n
}

//------------------------------------------------------------------------------

const (
	stateRunning = 0
	stateClosing = 1
	stateClosed  = 2
)

type Queue struct {
	opt *taskq.QueueOptions

	sync    bool
	noDelay bool

	wg       sync.WaitGroup
	consumer *taskq.Consumer

	scheduler scheduler

	_state int32
}

var _ taskq.Queue = (*Queue)(nil)

func NewQueue(opt *taskq.QueueOptions) *Queue {
	return NewQueueMaybeConsumer(true, opt)
}

func NewQueueMaybeConsumer(startConsumer bool, opt *taskq.QueueOptions) *Queue {
	opt.Init()

	q := &Queue{
		opt: opt,
	}

	q.consumer = taskq.NewConsumer(q)
	if startConsumer {
		if err := q.consumer.Start(context.Background()); err != nil {
			panic(err)
		}
	}

	return q
}

func (q *Queue) Name() string {
	return q.opt.Name
}

func (q *Queue) String() string {
	return fmt.Sprintf("queue=%q", q.Name())
}

func (q *Queue) Options() *taskq.QueueOptions {
	return q.opt
}

func (q *Queue) Consumer() taskq.QueueConsumer {
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
	if !atomic.CompareAndSwapInt32(&q._state, stateRunning, stateClosing) {
		return fmt.Errorf("taskq: %s is already closed", q)
	}
	err := q.WaitTimeout(timeout)

	if !atomic.CompareAndSwapInt32(&q._state, stateClosing, stateClosed) {
		panic("not reached")
	}

	_ = q.consumer.StopTimeout(timeout)
	_ = q.Purge()

	return err
}

func (q *Queue) WaitTimeout(timeout time.Duration) error {
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
	if q.isDuplicate(msg) {
		msg.Err = taskq.ErrDuplicate
		return nil
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

	if msg.Delay > 0 {
		q.scheduler.Schedule(msg, func() {
			// If the queue closed while we were waiting, just return
			if q.closed() {
				q.wg.Done()
				return
			}
			msg.Delay = 0
			_ = q.consumer.Add(msg)
		})
		return nil
	}
	return q.consumer.Add(msg)
}

func (q *Queue) ReserveN(_ context.Context, _ int, _ time.Duration) ([]taskq.Message, error) {
	return nil, internal.ErrNotSupported
}

func (q *Queue) Release(msg *taskq.Message) error {
	// Shallow copy.
	clone := *msg
	clone.Err = nil
	return q.enqueueMessage(&clone)
}

func (q *Queue) Delete(msg *taskq.Message) error {
	q.scheduler.Remove(msg)
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
	// Purge any messages already in the consumer
	err := q.consumer.Purge()

	numPurged := q.scheduler.Purge()
	for i := 0; i < numPurged; i++ {
		q.wg.Done()
	}

	return err
}

func (q *Queue) closed() bool {
	return atomic.LoadInt32(&q._state) == stateClosed
}

func (q *Queue) isDuplicate(msg *taskq.Message) bool {
	if msg.Name == "" {
		return false
	}
	return q.opt.Storage.Exists(msg.Ctx, msgutil.FullMessageName(q, msg))
}
