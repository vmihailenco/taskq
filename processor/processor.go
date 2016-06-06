package processor

import (
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"

	"gopkg.in/queue.v1"
)

const consumerBackoff = time.Second
const maxBackoff = 12 * time.Hour

type Limiter interface {
	AllowRate(name string, limit rate.Limit) (delay time.Duration, allow bool)
}

type Stats struct {
	Buffered    uint32
	Processed   uint32
	Errors      uint32
	Retries     uint32
	AvgDuration uint32
}

type Processor struct {
	q   Queuer
	opt *Options

	handler         queue.Handler
	fallbackHandler queue.Handler

	ch   chan *queue.Message
	wg   sync.WaitGroup
	stop uint32

	buffered    uint32
	processed   uint32
	errors      uint32
	retries     uint32
	avgDuration uint32
}

func New(q Queuer, opt *Options) *Processor {
	opt.init()

	p := &Processor{
		q:   q,
		opt: opt,

		ch: make(chan *queue.Message, opt.BufferSize),
	}
	p.SetHandler(opt.Handler)
	if opt.FallbackHandler != nil {
		p.SetFallbackHandler(opt.FallbackHandler)
	}
	return p
}

func Start(q Queuer, opt *Options) *Processor {
	p := New(q, opt)
	p.Start()
	return p
}

func (p *Processor) Start() error {
	p.wg.Add(1)
	go p.prefetchMessages()
	p.startWorkers()
	return nil
}

func (p *Processor) String() string {
	return fmt.Sprintf("Processor<%s>", p.q.Name())
}

func (p *Processor) Stats() *Stats {
	if p.stopped() {
		return nil
	}
	return &Stats{
		Buffered:    atomic.LoadUint32(&p.buffered),
		Processed:   atomic.LoadUint32(&p.processed),
		Errors:      atomic.LoadUint32(&p.errors),
		Retries:     atomic.LoadUint32(&p.retries),
		AvgDuration: atomic.LoadUint32(&p.avgDuration),
	}
}

func (p *Processor) SetHandler(handler interface{}) {
	p.handler = queue.NewHandler(handler)
}

func (p *Processor) SetFallbackHandler(handler interface{}) {
	p.fallbackHandler = queue.NewHandler(handler)
}

func (p *Processor) AddMessage(msg *queue.Message) error {
	p.ch <- msg
	return nil
}

func (p *Processor) startWorkers() {
	for i := 0; i < p.opt.WorkerNumber; i++ {
		p.wg.Add(1)
		go p.worker()
	}
}

func (p *Processor) Stop() error {
	return p.StopTimeout(30 * time.Second)
}

func (p *Processor) StopTimeout(timeout time.Duration) error {
	atomic.StoreUint32(&p.stop, 1)
	return p.waitWorkers(timeout)
}

func (p *Processor) stopped() bool {
	return atomic.LoadUint32(&p.stop) == 1
}

func (p *Processor) waitWorkers(timeout time.Duration) error {
	stopped := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(stopped)
	}()

	select {
	case <-time.After(timeout):
		return fmt.Errorf("workers did not stop after %s seconds", timeout)
	case <-stopped:
		return nil
	}
}

func (p *Processor) ProcessAll() error {
	p.startWorkers()
	var noWork int
	for {
		isIdle := atomic.LoadUint32(&p.buffered) == 0
		n, err := p.prefetch()
		if err != nil {
			return err
		}
		if n == 0 && isIdle {
			noWork++
		} else {
			noWork = 0
		}
		if noWork == 2 {
			break
		}
	}
	close(p.ch)
	return p.waitWorkers(time.Minute)
}

func (p *Processor) ProcessOne() error {
	msgs, err := p.q.ReserveN(1)
	if err != nil {
		return err
	}
	if len(msgs) == 0 {
		return errors.New("no messages in queue")
	}
	return p.Process(&msgs[0])
}

func (p *Processor) prefetchMessages() {
	defer p.wg.Done()
	for {
		if p.stopped() {
			close(p.ch)
			return
		}

		_, err := p.prefetch()
		if err != nil {
			log.Printf("%s ReserveN failed: %s (sleeping for %s)", p.q, err, consumerBackoff)
			time.Sleep(consumerBackoff)
			continue
		}
	}
}

func (p *Processor) prefetch() (int, error) {
	msgs, err := p.q.ReserveN(p.opt.BufferSize)
	if err != nil {
		return 0, err
	}
	atomic.AddUint32(&p.buffered, uint32(len(msgs)))
	for i := range msgs {
		p.ch <- &msgs[i]
	}
	return len(msgs), nil
}

func (p *Processor) worker() {
	defer p.wg.Done()
	for {
		if p.opt != nil && p.opt.Limiter != nil {
			delay, allow := p.opt.Limiter.AllowRate(p.q.Name(), p.opt.RateLimit)
			if !allow {
				time.Sleep(delay)
				continue
			}
		}

		msg, ok := <-p.ch
		if !ok {
			break
		}

		if msg.Delay > 0 {
			p.release(msg)
			continue
		}

		p.Process(msg)
	}
}

func (p *Processor) Process(msg *queue.Message) error {
	start := time.Now()
	err := p.handler.HandleMessage(msg)
	p.updateAvgDuration(time.Since(start))

	if err == nil {
		atomic.AddUint32(&p.processed, 1)
		p.delete(msg, nil)
		return nil
	}

	if msg.ReservedCount < p.opt.Retries {
		atomic.AddUint32(&p.retries, 1)
		log.Printf("%s handler failed on %s (will retry): %s", p.q, msg, err)
		p.release(msg)
	} else {
		atomic.AddUint32(&p.errors, 1)
		log.Printf("%s handler failed on %s: %s", p.q, msg, err)
		p.delete(msg, err)
	}

	return err
}

func (p *Processor) release(msg *queue.Message) {
	atomic.AddUint32(&p.buffered, ^uint32(0))

	var delay time.Duration
	if msg.Delay > 0 {
		delay = msg.Delay
	} else {
		delay = exponentialBackoff(p.opt.Backoff, msg.ReservedCount)
	}
	if err := p.q.Release(msg, delay); err != nil {
		log.Printf("Release failed: %s", err)
	}
}

func (p *Processor) delete(msg *queue.Message, reason error) {
	atomic.AddUint32(&p.buffered, ^uint32(0))

	if reason != nil && p.fallbackHandler != nil {
		if err := p.fallbackHandler.HandleMessage(msg); err != nil {
			log.Printf("%s fallback handler on failed %s: %s", p.q, msg, err)
		}
	}

	if err := p.q.Delete(msg, reason); err != nil {
		log.Printf("Delete failed: %s", err)
	}
}

func (p *Processor) updateAvgDuration(dur time.Duration) {
	const decay = float64(1) / 50

	ms := float64(dur / time.Millisecond)
	for {
		avg := atomic.LoadUint32(&p.avgDuration)
		newAvg := uint32((1-decay)*float64(avg) + decay*ms)
		if atomic.CompareAndSwapUint32(&p.avgDuration, avg, newAvg) {
			break
		}
	}
}

func exponentialBackoff(dur time.Duration, retry int) time.Duration {
	dur <<= uint(retry - 1)
	if dur > maxBackoff {
		dur = maxBackoff
	}
	return dur
}
