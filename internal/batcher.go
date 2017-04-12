package internal

import (
	"sync"
	"time"

	"github.com/go-msgqueue/msgqueue"
)

const batcherTimeout = time.Second

type Batcher struct {
	fn    func([]*msgqueue.Message)
	limit int

	wg sync.WaitGroup

	mu        sync.Mutex
	closed    bool
	msgs      []*msgqueue.Message
	lastMsgAt time.Time
}

func NewBatcher(limit int, fn func([]*msgqueue.Message)) *Batcher {
	b := Batcher{
		fn:    fn,
		limit: limit,
	}
	go b.callOnTimeout()
	return &b
}

func (b *Batcher) Wait() error {
	b.mu.Lock()
	b.wait()
	b.mu.Unlock()
	b.wg.Wait()
	return nil
}

func (b *Batcher) wait() {
	if len(b.msgs) > 0 {
		b.fn(b.msgs)
		b.msgs = nil
	}
}

func (b *Batcher) Close() error {
	b.mu.Lock()
	b.closed = true
	b.wait()
	b.mu.Unlock()
	return nil
}

func (b *Batcher) Add(msg *msgqueue.Message) {
	var msgs []*msgqueue.Message

	b.mu.Lock()
	b.msgs = append(b.msgs, msg)
	if len(b.msgs) > 0 && (len(b.msgs) > b.limit || b.timeoutReached()) {
		msgs = b.msgs
		b.msgs = nil
	}
	b.lastMsgAt = time.Now()
	b.mu.Unlock()

	if len(msgs) > 0 {
		b.fn(msgs)
	}
}

func (b *Batcher) callOnTimeout() {
	defer b.wg.Done()
	for {
		var closed bool
		var msgs []*msgqueue.Message

		b.mu.Lock()
		closed = b.closed
		if len(b.msgs) > 0 && b.timeoutReached() {
			msgs = b.msgs
			b.msgs = nil
		}
		b.mu.Unlock()

		if len(msgs) > 0 {
			b.wg.Add(1)
			go func() {
				defer b.wg.Done()
				b.fn(msgs)
			}()
		}
		if closed {
			break
		}

		time.Sleep(3 * batcherTimeout)
	}
}

func (b *Batcher) timeoutReached() bool {
	return time.Since(b.lastMsgAt) > batcherTimeout
}
