package memqueue

import (
	"fmt"
	"sync"
	"time"

	"github.com/go-msgqueue/msgqueue"
)

type manager struct{}

func (manager) NewQueue(opt *msgqueue.Options) msgqueue.Queue {
	return NewQueue(opt)
}

func (manager) Queues() []msgqueue.Queue {
	var queues []msgqueue.Queue
	for _, q := range Queues() {
		queues = append(queues, q)
	}
	return queues
}

func NewManager() msgqueue.Manager {
	return manager{}
}

type Queue struct {
	opt *msgqueue.Options

	sync    bool
	noDelay bool

	p *msgqueue.Processor

	wg   sync.WaitGroup
	msgs chan *msgqueue.Message
}

var _ msgqueue.Queue = (*Queue)(nil)

func NewQueue(opt *msgqueue.Options) *Queue {
	opt.Init()
	bufferSize := opt.BufferSize
	opt.BufferSize = 1
	opt.WaitTimeout = 100 * time.Millisecond

	q := Queue{
		opt:  opt,
		msgs: make(chan *msgqueue.Message, bufferSize),
	}
	q.p = msgqueue.StartProcessor(&q, opt)

	registerQueue(&q)
	return &q
}

func (q *Queue) Name() string {
	return q.opt.Name
}

func (q *Queue) String() string {
	return fmt.Sprintf("Memqueue<Name=%s>", q.Name())
}

func (q *Queue) Options() *msgqueue.Options {
	return q.opt
}

func (q *Queue) Processor() *msgqueue.Processor {
	return q.p
}

func (q *Queue) SetSync(sync bool) {
	q.sync = sync
}

func (q *Queue) SetNoDelay(noDelay bool) {
	q.noDelay = noDelay
}

// Close is CloseTimeout with 30 seconds timeout.
func (q *Queue) Close() error {
	return q.CloseTimeout(30 * time.Second)
}

// Close closes the queue waiting for pending messages to be processed.
func (q *Queue) CloseTimeout(timeout time.Duration) error {
	defer unregisterQueue(q)

	done := make(chan struct{}, 1)
	timeoutCh := time.After(timeout)

	go func() {
		q.wg.Wait()
		done <- struct{}{}
	}()

	select {
	case <-timeoutCh:
		return fmt.Errorf("messages were not consumed after %s", timeout)
	case <-done:
	}

	return q.p.StopTimeout(timeout)
}

// Add adds message to the queue.
func (q *Queue) Add(msg *msgqueue.Message) error {
	return q.addMessage(msg)
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

func (q *Queue) addMessage(msg *msgqueue.Message) error {
	if !q.isUniqueName(msg.Name) {
		return msgqueue.ErrDuplicate
	}
	q.wg.Add(1)
	return q.enqueueMessage(msg)
}

func (q *Queue) enqueueMessage(msg *msgqueue.Message) error {
	var delay time.Duration
	delay, msg.Delay = msg.Delay, 0
	msg.ReservedCount++

	if q.sync {
		return q.p.Process(msg)
	}

	if q.noDelay || delay == 0 {
		q.msgs <- msg
		return nil
	}

	time.AfterFunc(delay, func() {
		q.msgs <- msg
	})
	return nil
}

func (q *Queue) isUniqueName(name string) bool {
	const redisPrefix = "memqueue"

	if name == "" {
		return true
	}
	key := fmt.Sprintf("%s:%s:%s", redisPrefix, q.opt.GroupName, name)
	exists := q.opt.Storage.Exists(key)
	return !exists
}

func (q *Queue) ReserveN(n int) ([]*msgqueue.Message, error) {
	select {
	case msg := <-q.msgs:
		return []*msgqueue.Message{msg}, nil
	case <-time.After(q.opt.WaitTimeout):
		return nil, nil
	}
}

func (q *Queue) Release(msg *msgqueue.Message, dur time.Duration) error {
	msg.Delay = dur
	return q.enqueueMessage(msg)
}

func (q *Queue) Delete(msg *msgqueue.Message) error {
	q.wg.Done()
	return nil
}

func (q *Queue) DeleteBatch(msgs []*msgqueue.Message) error {
	for _, msg := range msgs {
		if err := q.Delete(msg); err != nil {
			return err
		}
	}
	return nil
}

func (q *Queue) Purge() error {
loop:
	for {
		select {
		case <-q.msgs:
		default:
			break loop
		}
	}
	return q.p.Purge()
}
