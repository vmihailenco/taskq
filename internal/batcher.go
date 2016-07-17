package internal

import (
	"sync"
	"time"

	"gopkg.in/queue.v1"
)

type Batcher struct {
	fn    func([]*queue.Message)
	limit chan struct{}
	ch    chan *queue.Message
	wg    sync.WaitGroup
}

func NewBatcher(limit int, fn func([]*queue.Message)) *Batcher {
	b := Batcher{
		fn:    fn,
		limit: make(chan struct{}, limit),
		ch:    make(chan *queue.Message, limit),
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

func (b *Batcher) Add(msg *queue.Message) {
	b.wg.Add(1)
	b.ch <- msg
}

func (b *Batcher) batcher() {
	var msgs []*queue.Message
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
			go func(msgs []*queue.Message) {
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
