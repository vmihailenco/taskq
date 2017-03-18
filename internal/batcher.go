package internal

import (
	"sync"
	"time"

	"github.com/go-msgqueue/msgqueue"
)

type Batcher struct {
	fn    func([]*msgqueue.Message)
	limit chan struct{}
	ch    chan *msgqueue.Message
	wg    sync.WaitGroup
}

func NewBatcher(limit int, fn func([]*msgqueue.Message)) *Batcher {
	b := Batcher{
		fn:    fn,
		limit: make(chan struct{}, limit),
		ch:    make(chan *msgqueue.Message, limit),
	}
	go b.batcher()
	return &b
}

func (b *Batcher) Wait() error {
	b.wg.Wait()
	return nil
}

func (b *Batcher) Close() error {
	close(b.ch)
	return b.Wait()
}

func (b *Batcher) Add(msg *msgqueue.Message) {
	b.wg.Add(1)
	b.ch <- msg
}

func (b *Batcher) batcher() {
	var msgs []*msgqueue.Message
	for {
		var stop, timeout bool
		select {
		case msg, ok := <-b.ch:
			if ok {
				msgs = append(msgs, msg)
			} else {
				stop = true
			}
		case <-time.After(time.Second):
			timeout = true
		}

		if (timeout && len(msgs) > 0) || len(msgs) >= 10 {
			b.limit <- struct{}{}
			go func(msgs []*msgqueue.Message) {
				b.fn(msgs)
				<-b.limit
				for i := 0; i < len(msgs); i++ {
					b.wg.Done()
				}
			}(msgs)
			msgs = nil
		}

		if stop {
			break
		}
	}
}
