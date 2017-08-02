package msgqueue

import (
	"sync"
	"sync/atomic"
	"time"
)

const batcherTimeout = 3 * time.Second

type msgBatcher struct {
	limit int
	fn    func([]*Message)
	_sync uint32 // atomic

	wg sync.WaitGroup

	mu         sync.Mutex
	closed     bool
	msgs       []*Message
	firstMsgAt time.Time
}

func newMsgBatcher(limit int, fn func([]*Message)) *msgBatcher {
	b := msgBatcher{
		limit: limit,
		fn:    fn,
	}
	go b.callOnTimeout()
	return &b
}

func (b *msgBatcher) SetSync(v bool) {
	var n uint32
	if v {
		n = 1
	}
	atomic.StoreUint32(&b._sync, n)

	if !v {
		return
	}

	b.mu.Lock()
	if len(b.msgs) > 0 {
		b.fn(b.msgs)
		b.msgs = nil
	}
	b.mu.Unlock()
}

func (b *msgBatcher) isSync() bool {
	return atomic.LoadUint32(&b._sync) == 1
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
	if b.isSync() || len(b.msgs) >= b.limit || b.timeoutReached() {
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
			b.wg.Add(1)
			go func(msgs []*Message) {
				defer b.wg.Done()
				b.fn(msgs)
			}(b.msgs)
			b.msgs = nil
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
