package msgqueue

import (
	"sync"
	"time"
)

const batcherTimeout = 3 * time.Second

type msgBatcher struct {
	fn    func([]*Message)
	limit int

	wg sync.WaitGroup

	mu         sync.Mutex
	closed     bool
	msgs       []*Message
	firstMsgAt time.Time
}

func newMsgBatcher(limit int, fn func([]*Message)) *msgBatcher {
	b := msgBatcher{
		fn: fn,
	}
	b.SetLimit(limit)
	go b.callOnTimeout()
	return &b
}

func (b *msgBatcher) SetLimit(limit int) {
	const maxLimit = 10
	if limit > maxLimit {
		limit = maxLimit
	}

	b.mu.Lock()
	b.limit = limit
	b.mu.Unlock()
}

func (b *msgBatcher) Wait() error {
	b.mu.Lock()
	b.wait()
	b.mu.Unlock()
	b.wg.Wait()
	return nil
}

func (b *msgBatcher) wait() {
	if len(b.msgs) > 0 {
		b.fn(b.msgs)
		b.msgs = nil
	}
}

func (b *msgBatcher) Close() error {
	b.mu.Lock()
	b.closed = true
	b.wait()
	b.mu.Unlock()
	return nil
}

func (b *msgBatcher) Add(msg *Message) {
	var msgs []*Message

	b.mu.Lock()
	if len(b.msgs) == 0 {
		b.firstMsgAt = time.Now()
	}
	b.msgs = append(b.msgs, msg)
	if len(b.msgs) >= b.limit || b.timeoutReached() {
		msgs = b.msgs
		b.msgs = nil
	}
	b.mu.Unlock()

	if len(msgs) > 0 {
		b.fn(msgs)
	}
}

func (b *msgBatcher) callOnTimeout() {
	for {
		b.mu.Lock()
		if b.timeoutReached() {
			msgs := b.msgs
			b.msgs = nil

			b.wg.Add(1)
			go func() {
				defer b.wg.Done()
				b.fn(msgs)
			}()
		}
		closed := b.closed
		b.mu.Unlock()

		if closed {
			break
		}

		time.Sleep(batcherTimeout)
	}
}

func (b *msgBatcher) timeoutReached() bool {
	return len(b.msgs) > 0 && time.Since(b.firstMsgAt) > batcherTimeout
}
