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
	"gopkg.in/queue.v1/internal"
)

const consumerBackoff = time.Second
const maxBackoff = 12 * time.Hour

type Limiter interface {
	AllowRate(name string, limit rate.Limit) (delay time.Duration, allow bool)
}

type Delayer interface {
	Delay() time.Duration
}

type Stats struct {
	InFlight    uint32
	Deleting    uint32
	Processed   uint32
	Retries     uint32
	Fails       uint32
	AvgDuration time.Duration
}

type Processor struct {
	q   Queuer
	opt *Options

	handler         queue.Handler
	fallbackHandler queue.Handler

	ch chan *queue.Message
	wg sync.WaitGroup

	delBatch *internal.Batcher

	stop uint32

	inFlight    uint32
	deleting    uint32
	processed   uint32
	fails       uint32
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
	p.setHandler(opt.Handler)
	if opt.FallbackHandler != nil {
		p.setFallbackHandler(opt.FallbackHandler)
	}
	p.delBatch = internal.NewBatcher(opt.Scavengers, p.deleteBatch)
	return p
}

func Start(q Queuer, opt *Options) *Processor {
	p := New(q, opt)
	p.Start()
	return p
}

func (p *Processor) Start() error {
	p.wg.Add(1)
	go p.messageFetcher()

	p.startWorkers()

	return nil
}

func (p *Processor) String() string {
	return fmt.Sprintf(
		"Processor<%s workers=%d scavengers=%d buffer=%d>",
		p.q.Name(), p.opt.Workers, p.opt.Scavengers, p.opt.BufferSize,
	)
}

func (p *Processor) Stats() *Stats {
	if p.stopped() {
		return nil
	}
	return &Stats{
		InFlight:    atomic.LoadUint32(&p.inFlight),
		Deleting:    atomic.LoadUint32(&p.deleting),
		Processed:   atomic.LoadUint32(&p.processed),
		Retries:     atomic.LoadUint32(&p.retries),
		Fails:       atomic.LoadUint32(&p.fails),
		AvgDuration: time.Duration(atomic.LoadUint32(&p.avgDuration)) * time.Millisecond,
	}
}

func (p *Processor) setHandler(handler interface{}) {
	p.handler = queue.NewHandler(handler)
}

func (p *Processor) setFallbackHandler(handler interface{}) {
	p.fallbackHandler = queue.NewHandler(handler)
}

func (p *Processor) Add(msg *queue.Message) error {
	p.ch <- msg
	return nil
}

func (p *Processor) startWorkers() {
	p.wg.Add(p.opt.Workers)
	for i := 0; i < p.opt.Workers; i++ {
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
		p.delBatch.Close()
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
		isIdle := atomic.LoadUint32(&p.inFlight) == 0
		n, err := p.fetchMessages()
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

func (p *Processor) messageFetcher() {
	defer p.wg.Done()
	for {
		if p.stopped() {
			close(p.ch)
			break
		}

		_, err := p.fetchMessages()
		if err != nil {
			log.Printf("%s ReserveN failed: %s (sleeping for %s)", p.q, err, consumerBackoff)
			time.Sleep(consumerBackoff)
			continue
		}
	}
}

func (p *Processor) fetchMessages() (int, error) {
	msgs, err := p.q.ReserveN(p.opt.BufferSize)
	if err != nil {
		return 0, err
	}
	atomic.AddUint32(&p.inFlight, uint32(len(msgs)))
	for i := range msgs {
		p.ch <- &msgs[i]
	}
	return len(msgs), nil
}

func (p *Processor) worker() {
	defer p.wg.Done()
	for {
		if p.opt.Limiter != nil {
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

		if !p.opt.IgnoreMessageDelay && msg.Delay > 0 {
			p.release(msg, nil)
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

	msg.SetValue("err", err)
	if msg.ReservedCount < p.opt.Retries {
		atomic.AddUint32(&p.retries, 1)
		p.release(msg, err)
	} else {
		atomic.AddUint32(&p.fails, 1)
		p.delete(msg, err)
	}

	return msg.Value("err").(error)
}

func (p *Processor) release(msg *queue.Message, reason error) {
	delay := p.backoff(msg, reason)

	if reason != nil {
		log.Printf("%s handler failed (retry in %s): %s", p.q, delay, reason)
	}
	if err := p.q.Release(msg, delay); err != nil {
		log.Printf("%s Release failed: %s", p.q, err)
	}

	atomic.AddUint32(&p.inFlight, ^uint32(0))
}

func (p *Processor) backoff(msg *queue.Message, reason error) time.Duration {
	if reason != nil {
		if delayer, ok := reason.(Delayer); ok {
			return delayer.Delay()
		}
	}
	if !p.opt.IgnoreMessageDelay && msg.Delay > 0 {
		return msg.Delay
	}
	return exponentialBackoff(p.opt.Backoff, msg.ReservedCount)
}

func (p *Processor) delete(msg *queue.Message, reason error) {
	if reason != nil {
		log.Printf("%s handler failed: %s", p.q, reason)

		if p.fallbackHandler != nil {
			if err := p.fallbackHandler.HandleMessage(msg); err != nil {
				log.Printf("%s fallback handler failed: %s", p.q, err)
			}
		}
	}

	atomic.AddUint32(&p.inFlight, ^uint32(0))
	atomic.AddUint32(&p.deleting, 1)
	p.delBatch.Add(msg)
}

func (p *Processor) deleteBatch(msgs []*queue.Message) {
	if err := p.q.DeleteBatch(msgs); err != nil {
		log.Printf("%s DeleteBatch failed: %s", p.q, err)
	}
	atomic.AddUint32(&p.deleting, ^uint32(len(msgs)-1))
}

func (p *Processor) updateAvgDuration(dur time.Duration) {
	const decay = float64(1) / 100

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
