package msgqueue

import (
	"sync"
	"sync/atomic"
	"time"
)

type BatcherOptions struct {
	Worker   func([]*Message) error
	Splitter func([]*Message) ([]*Message, []*Message)
	Timeout  time.Duration
}

func (opt *BatcherOptions) init() {
	if opt.Timeout == 0 {
		opt.Timeout = 3 * time.Second
	}
}

type Batcher struct {
	q   Queue
	opt *BatcherOptions

	_sync uint32 // atomic
	wg    sync.WaitGroup

	mu         sync.Mutex
	closed     bool
	msgs       []*Message
	firstMsgAt time.Time
}

func NewBatcher(q Queue, opt *BatcherOptions) *Batcher {
	opt.init()
	b := Batcher{
		q:   q,
		opt: opt,
	}
	go b.callOnTimeout()
	return &b
}

func (b *Batcher) SetSync(v bool) {
	var n uint32
	if v {
		n = 1
	}
	atomic.StoreUint32(&b._sync, n)

	if !v {
		return
	}

	b.mu.Lock()
	b.wait()
	b.mu.Unlock()
}

func (b *Batcher) isSync() bool {
	return atomic.LoadUint32(&b._sync) == 1
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
		b.process(b.msgs)
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

func (b *Batcher) Add(msg *Message) {
	var msgs []*Message

	b.mu.Lock()

	if len(b.msgs) == 0 {
		b.firstMsgAt = time.Now()
	}
	b.msgs = append(b.msgs, msg)

	if b.isSync() || b.timeoutReached() {
		msgs = b.msgs
		b.msgs = nil
	} else {
		b.msgs, msgs = b.opt.Splitter(b.msgs)
	}

	b.mu.Unlock()

	if len(msgs) > 0 {
		b.process(msgs)
	}
}

func (b *Batcher) process(msgs []*Message) {
	_ = b.opt.Worker(msgs)
	for _, msg := range msgs {
		if msg.Err == nil {
			b.q.Delete(msg)
		} else {
			b.q.Release(msg)
		}
	}
}

func (b *Batcher) callOnTimeout() {
	for {
		b.mu.Lock()
		if b.timeoutReached() {
			b.wg.Add(1)
			go func(msgs []*Message) {
				defer b.wg.Done()
				b.process(msgs)
			}(b.msgs)
			b.msgs = nil
		}
		closed := b.closed
		b.mu.Unlock()

		if closed {
			break
		}

		time.Sleep(b.opt.Timeout)
	}
}

func (b *Batcher) timeoutReached() bool {
	return len(b.msgs) > 0 && time.Since(b.firstMsgAt) > b.opt.Timeout
}
