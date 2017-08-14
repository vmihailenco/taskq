package msgqueue

import (
	"errors"
	"sync"
	"time"
)

var errBatched = errors.New("message is batched")
var errBatchProcessed = errors.New("message is processed in a batch")

type BatcherOptions struct {
	Handler  func([]*Message) error
	Splitter func([]*Message) ([]*Message, []*Message)

	RetryLimit int
	Timeout    time.Duration
}

func (opt *BatcherOptions) init(p *Processor) {
	if opt.RetryLimit == 0 {
		opt.RetryLimit = p.Options().RetryLimit
	}
	if opt.Timeout == 0 {
		opt.Timeout = 3 * time.Second
	}
}

// Batcher collects messages for later batch processing.
type Batcher struct {
	p   *Processor
	opt *BatcherOptions

	timer *time.Timer

	mu   sync.Mutex
	msgs []*Message
	sync bool
}

func NewBatcher(p *Processor, opt *BatcherOptions) *Batcher {
	opt.init(p)
	b := Batcher{
		p:   p,
		opt: opt,
	}
	b.timer = time.AfterFunc(time.Minute, b.onTimeout)
	b.timer.Stop()
	return &b
}

func (b *Batcher) SetSync(v bool) {
	b.mu.Lock()
	b.sync = v
	if v {
		b.wait()
	}
	b.mu.Unlock()
}

func (b *Batcher) wait() {
	if len(b.msgs) > 0 {
		b.process(b.msgs)
		b.msgs = nil
	}
}

func (b *Batcher) Add(msg *Message) error {
	var msgs []*Message

	b.mu.Lock()

	if len(b.msgs) == 0 {
		b.stopTimer()
		b.timer.Reset(b.opt.Timeout)
	}

	b.msgs = append(b.msgs, msg)
	if b.sync {
		msgs = b.msgs
		b.msgs = nil
	} else {
		msgs, b.msgs = b.opt.Splitter(b.msgs)
	}

	b.mu.Unlock()

	if len(msgs) > 0 {
		b.process(msgs)
		return errBatchProcessed
	}

	return errBatched
}

func (b *Batcher) stopTimer() {
	if !b.timer.Stop() {
		select {
		case <-b.timer.C:
		default:
		}
	}
}

func (b *Batcher) process(msgs []*Message) {
	err := b.opt.Handler(msgs)
	for _, msg := range msgs {
		if msg.Err == nil && err != nil {
			msg.Err = err
		}
		b.p.Put(msg)
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
