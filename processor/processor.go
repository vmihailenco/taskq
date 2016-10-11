package processor

import (
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"gopkg.in/queue.v1"
	"gopkg.in/queue.v1/internal"
)

const consumerBackoff = time.Second
const maxBackoff = 12 * time.Hour
const stopTimeout = 30 * time.Second

var ErrNotSupported = errors.New("processor: not supported")

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
	opt *queue.Options

	handler         queue.Handler
	fallbackHandler queue.Handler

	ch chan *queue.Message
	wg sync.WaitGroup

	delBatch *internal.Batcher

	_started uint32
	stop     chan struct{}

	errCount   uint32
	delayCount uint32
	delaySec   uint32

	inFlight    uint32
	deleting    uint32
	processed   uint32
	fails       uint32
	retries     uint32
	avgDuration uint32
}

func New(q Queuer, opt *queue.Options) *Processor {
	opt.Init()
	p := &Processor{
		q:   q,
		opt: opt,

		ch: make(chan *queue.Message, opt.BufferSize),
	}

	p.setHandler(opt.Handler)
	if opt.FallbackHandler != nil {
		p.setFallbackHandler(opt.FallbackHandler)
	}

	p.delBatch = internal.NewBatcher(p.opt.ScavengerNumber, p.deleteBatch)

	return p
}

func Start(q Queuer, opt *queue.Options) *Processor {
	p := New(q, opt)
	p.Start()
	return p
}

func (p *Processor) String() string {
	return fmt.Sprintf(
		"Processor<%s workers=%d scavengers=%d buffer=%d>",
		p.q.Name(), p.opt.WorkerNumber, p.opt.ScavengerNumber, p.opt.BufferSize,
	)
}

func (p *Processor) Stats() *Stats {
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
	p.queueMessage(msg)
	return nil
}

func (p *Processor) Start() error {
	if !p.startWorkers() {
		return nil
	}

	p.wg.Add(1)
	go p.messageFetcher()

	return nil
}

func (p *Processor) startWorkers() bool {
	if !atomic.CompareAndSwapUint32(&p._started, 0, 1) {
		return false
	}

	p.stop = make(chan struct{})
	p.wg.Add(p.opt.WorkerNumber)
	for i := 0; i < p.opt.WorkerNumber; i++ {
		go p.worker()
	}
	return true
}

func (p *Processor) Stop() error {
	return p.StopTimeout(stopTimeout)
}

func (p *Processor) StopTimeout(timeout time.Duration) error {
	return p.stopWorkersTimeout(timeout)
}

func (p *Processor) stopWorkersTimeout(timeout time.Duration) error {
	if !atomic.CompareAndSwapUint32(&p._started, 1, 0) {
		return nil
	}

	close(p.stop)

	stopped := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(stopped)
	}()

	select {
	case <-time.After(timeout):
		return fmt.Errorf("workers did not stop after %s seconds", timeout)
	case <-stopped:
		return p.delBatch.Wait()
	}
}

func (p *Processor) stopped() bool {
	return atomic.LoadUint32(&p._started) == 0
}

func (p *Processor) paused() time.Duration {
	const threshold = 100

	if atomic.LoadUint32(&p.delayCount) > threshold {
		return time.Duration(atomic.LoadUint32(&p.delaySec)) * time.Second
	}

	if atomic.LoadUint32(&p.errCount) > threshold {
		return time.Minute
	}

	return 0
}

func (p *Processor) ProcessAll() error {
	p.startWorkers()
	var noWork int
	for {
		isIdle := atomic.LoadUint32(&p.inFlight) == 0
		n, err := p.fetchMessages()
		if err != nil {
			if err == ErrNotSupported {
				break
			}
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
	return p.stopWorkersTimeout(stopTimeout)
}

func (p *Processor) ProcessOne() error {
	msg, err := p.reserveOne()
	if err != nil {
		return err
	}
	retErr := p.Process(msg)
	if err := p.delBatch.Wait(); err != nil && retErr == nil {
		retErr = err
	}
	return retErr
}

func (p *Processor) reserveOne() (*queue.Message, error) {
	select {
	case msg := <-p.ch:
		return msg, nil
	default:
	}

	msgs, err := p.q.ReserveN(1)
	if err != nil && err != ErrNotSupported {
		return nil, err
	}
	if len(msgs) == 0 {
		return nil, errors.New("queue is empty")
	}
	atomic.AddUint32(&p.inFlight, 1)
	return &msgs[0], nil
}

func (p *Processor) messageFetcher() {
	defer p.wg.Done()
	for {
		if p.stopped() {
			break
		}

		if pauseTime := p.paused(); pauseTime > 0 {
			p.resetPause()
			log.Printf("%s is automatically paused for %s", p.q, pauseTime)
			time.Sleep(pauseTime)
			continue
		}

		_, err := p.fetchMessages()
		if err != nil {
			if err == ErrNotSupported {
				break
			}

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
	for i := range msgs {
		p.queueMessage(&msgs[i])
	}
	return len(msgs), nil
}

func (p *Processor) worker() {
	defer p.wg.Done()
	for {
		msg, ok := p.dequeueMessage()
		if !ok {
			break
		}

		if p.opt.RateLimiter != nil {
			for {
				delay, allow := p.opt.RateLimiter.AllowRate(p.q.Name(), p.opt.RateLimit)
				if allow {
					break
				}
				time.Sleep(delay)
			}
		}

		p.Process(msg)
	}
}

func (p *Processor) Process(msg *queue.Message) error {
	if msg.Delay > 0 {
		p.release(msg, nil)
		return nil
	}

	start := time.Now()
	err := p.handler.HandleMessage(msg)
	msg.SetValue("err", err)
	p.updateAvgDuration(time.Since(start))

	if err == nil {
		atomic.AddUint32(&p.processed, 1)
		p.delete(msg, nil)
		return nil
	}

	if msg.ReservedCount < p.opt.RetryLimit {
		atomic.AddUint32(&p.retries, 1)
		p.release(msg, err)
	} else {
		atomic.AddUint32(&p.fails, 1)
		p.delete(msg, err)
	}

	if v := msg.Value("err"); v != nil {
		return v.(error)
	}
	return nil
}

func (p *Processor) Purge() error {
	for {
		select {
		case msg := <-p.ch:
			p.delete(msg, nil)
		default:
			return nil
		}
	}
}

func (p *Processor) queueMessage(msg *queue.Message) {
	atomic.AddUint32(&p.inFlight, 1)
	p.ch <- msg
}

func (p *Processor) dequeueMessage() (*queue.Message, bool) {
	select {
	case msg := <-p.ch:
		return msg, true
	case <-p.stop:
		select {
		case msg := <-p.ch:
			return msg, true
		default:
			return nil, false
		}
	}
}

func (p *Processor) release(msg *queue.Message, reason error) {
	delay := p.releaseBackoff(msg, reason)

	if reason != nil {
		log.Printf("%s handler failed (retry in %s): %s", p.q, delay, reason)
	}
	if err := p.q.Release(msg, delay); err != nil {
		log.Printf("%s Release failed: %s", p.q, err)
	}

	atomic.AddUint32(&p.inFlight, ^uint32(0))
}

func (p *Processor) releaseBackoff(msg *queue.Message, reason error) time.Duration {
	if reason != nil {
		if delayer, ok := reason.(Delayer); ok {
			delay := delayer.Delay()
			if delay > time.Minute {
				atomic.StoreUint32(&p.delaySec, uint32(delay/time.Second))
				atomic.AddUint32(&p.delayCount, 1)
			}
			return delay
		}
	}

	if msg.Delay > 0 {
		return msg.Delay
	}

	return exponentialBackoff(p.opt.MinBackoff, msg.ReservedCount)
}

func (p *Processor) delete(msg *queue.Message, reason error) {
	if reason == nil {
		p.resetPause()
	} else {
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

func (p *Processor) resetPause() {
	atomic.StoreUint32(&p.errCount, 0)
	atomic.StoreUint32(&p.delayCount, 0)
}

func exponentialBackoff(dur time.Duration, retry int) time.Duration {
	dur <<= uint(retry - 1)
	if dur > maxBackoff {
		dur = maxBackoff
	}
	return dur
}
