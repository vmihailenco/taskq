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

	timer *time.Timer

	_sync uint32 // atomic
	wg    sync.WaitGroup

	mu   sync.Mutex
	msgs []*Message
}

func NewBatcher(q Queue, opt *BatcherOptions) *Batcher {
	opt.init()
	b := Batcher{
		q:   q,
		opt: opt,
	}
	b.timer = time.AfterFunc(time.Minute, b.onTimeout)
	b.timer.Stop()
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

func (b *Batcher) wait() {
	if len(b.msgs) > 0 {
		b.process(b.msgs)
		b.msgs = nil
	}
}

func (b *Batcher) isSync() bool {
	return atomic.LoadUint32(&b._sync) == 1
}

func (b *Batcher) Add(msg *Message) {
	var msgs []*Message

	b.mu.Lock()

	if len(b.msgs) == 0 {
		b.stopTimer()
		b.timer.Reset(b.opt.Timeout)
	}
	b.msgs = append(b.msgs, msg)

	if b.isSync() {
		msgs = b.msgs
		b.msgs = nil
	} else {
		msgs, b.msgs = b.opt.Splitter(b.msgs)
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

func (b *Batcher) onTimeout() {
	b.mu.Lock()
	b.wait()
	b.mu.Unlock()
}

func (b *Batcher) Close() error {
	b.mu.Lock()
	b.stopTimer()
	b.wait()
	b.mu.Unlock()
	return nil
}

func (b *Batcher) stopTimer() {
	if !b.timer.Stop() {
		select {
		case <-b.timer.C:
		default:
		}
	}
}
