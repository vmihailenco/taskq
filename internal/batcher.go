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
	b.wg.Add(1)
	go b.batcher()
	return &b
}

func (b *Batcher) Close() error {
	close(b.ch)
	b.wg.Wait()
	return nil
}

func (b *Batcher) Add(msg *queue.Message) {
	b.ch <- msg
}

func (b *Batcher) batcher() {
	defer b.wg.Done()
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
			b.wg.Add(1)
			go func(msgs []*queue.Message) {
				defer func() {
					b.wg.Done()
					<-b.limit
				}()
				b.fn(msgs)
			}(msgs)
			msgs = nil
		}

		if stop {
			break
		}
	}
}
