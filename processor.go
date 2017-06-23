package msgqueue

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bsm/redis-lock"

	"github.com/go-msgqueue/msgqueue/internal"
)

const stopTimeout = 30 * time.Second

type Delayer interface {
	Delay() time.Duration
}

type ProcessorStats struct {
	InFlight    uint32
	Deleting    uint32
	Processed   uint32
	Retries     uint32
	Fails       uint32
	AvgDuration time.Duration
}

// Processor reserves messages from the queue, processes them,
// and then either releases or deletes messages from the queue.
type Processor struct {
	q   Queue
	opt *Options

	handler         Handler
	fallbackHandler Handler

	wg sync.WaitGroup
	ch chan *Message

	workersWG   sync.WaitGroup
	workerLocks []*lock.Lock

	workersStarted uint32
	workersStop    chan struct{}

	messageFetcherStarted uint32
	messageFetcherStop    uint32

	delBatch *msgBatcher

	errCount uint32
	delaySec uint32

	inFlight    uint32
	deleting    uint32
	processed   uint32
	fails       uint32
	retries     uint32
	avgDuration uint32
}

// New creates new Processor for the queue using provided processing options.
func NewProcessor(q Queue, opt *Options) *Processor {
	opt.Init()
	p := &Processor{
		q:   q,
		opt: opt,

		ch: make(chan *Message, opt.BufferSize-1),
	}

	if opt.WorkerLimit > 0 {
		p.workerLocks = make([]*lock.Lock, opt.WorkerLimit)
		for i := 0; i < opt.WorkerLimit; i++ {
			key := fmt.Sprintf("%s:worker-lock:%d", p.q.Name(), i)
			p.workerLocks[i] = lock.NewLock(opt.Redis, key, &lock.LockOptions{
				LockTimeout: opt.ReservationTimeout,
			})
		}
	}

	p.setHandler(opt.Handler)
	if opt.FallbackHandler != nil {
		p.setFallbackHandler(opt.FallbackHandler)
	}

	p.delBatch = newMsgBatcher(p.opt.BufferSize, p.deleteBatch)

	return p
}

// Starts creates new Processor and starts it.
func StartProcessor(q Queue, opt *Options) *Processor {
	p := NewProcessor(q, opt)
	p.Start()
	return p
}

func (p *Processor) String() string {
	return fmt.Sprintf(
		"Processor<%s workers=%d buffer=%d>",
		p.q.Name(), p.opt.WorkerNumber, p.opt.BufferSize,
	)
}

// Stats returns processor stats.
func (p *Processor) Stats() *ProcessorStats {
	return &ProcessorStats{
		InFlight:    atomic.LoadUint32(&p.inFlight),
		Deleting:    atomic.LoadUint32(&p.deleting),
		Processed:   atomic.LoadUint32(&p.processed),
		Retries:     atomic.LoadUint32(&p.retries),
		Fails:       atomic.LoadUint32(&p.fails),
		AvgDuration: time.Duration(atomic.LoadUint32(&p.avgDuration)) * time.Millisecond,
	}
}

func (p *Processor) setHandler(handler interface{}) {
	p.handler = NewHandler(handler)
}

func (p *Processor) setFallbackHandler(handler interface{}) {
	p.fallbackHandler = NewHandler(handler)
}

// Add adds message to the processor internal queue.
func (p *Processor) Add(msg *Message) error {
	p.wg.Add(1)
	atomic.AddUint32(&p.inFlight, 1)
	p.ch <- msg
	return nil
}

// Add adds message to the processor internal queue with specified delay.
func (p *Processor) AddDelay(msg *Message, delay time.Duration) error {
	if delay == 0 {
		return p.Add(msg)
	}

	p.wg.Add(1)
	atomic.AddUint32(&p.inFlight, 1)
	time.AfterFunc(delay, func() {
		p.ch <- msg
	})
	return nil
}

// Process is low-level API to process message bypassing the internal queue.
func (p *Processor) Process(msg *Message) error {
	p.wg.Add(1)
	return p.process(-1, msg)
}

// Start starts processing messages in the queue.
func (p *Processor) Start() error {
	p.startWorkers()
	return nil
}

func (p *Processor) startWorkers() bool {
	if !atomic.CompareAndSwapUint32(&p.workersStarted, 0, 1) {
		return false
	}

	p.workersStop = make(chan struct{})
	for i := 0; i < p.opt.WorkerNumber; i++ {
		p.workersWG.Add(1)
		go p.worker(i, p.workersStop)
	}

	return true
}

// Stop is StopTimeout with 30 seconds timeout.
func (p *Processor) Stop() error {
	return p.StopTimeout(stopTimeout)
}

// StopTimeout waits workers for timeout duration to finish processing current
// messages and stops workers.
func (p *Processor) StopTimeout(timeout time.Duration) error {
	if !atomic.CompareAndSwapUint32(&p.workersStarted, 1, 0) {
		return nil
	}

	atomic.StoreUint32(&p.messageFetcherStop, 1)
	defer atomic.StoreUint32(&p.messageFetcherStarted, 0)

	p.delBatch.SetLimit(1)
	defer p.delBatch.SetLimit(p.opt.WorkerNumber)
	p.delBatch.Wait()

	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()

	select {
	case <-time.After(timeout):
		return fmt.Errorf("messages were not processed after %s", timeout)
	case <-done:
	}

	close(p.workersStop)
	p.workersStop = nil

	done = make(chan struct{})
	go func() {
		p.workersWG.Wait()
		p.delBatch.Wait()
		close(done)
	}()

	select {
	case <-time.After(timeout):
		return fmt.Errorf("workers were not stopped after %s", timeout)
	case <-done:
		return nil
	}
}

func (p *Processor) paused() time.Duration {
	const threshold = 100

	if p.opt.PauseErrorsThreshold == 0 ||
		atomic.LoadUint32(&p.errCount) < uint32(p.opt.PauseErrorsThreshold) {
		return 0
	}

	sec := atomic.LoadUint32(&p.delaySec)
	if sec == 0 {
		return time.Minute
	}
	return time.Duration(sec) * time.Second
}

// ProcessAll starts workers to process messages in the queue and then stops
// them when all messages are processed.
func (p *Processor) ProcessAll() error {
	p.startWorkers()

	var noWork int
	for {
		isIdle := atomic.LoadUint32(&p.inFlight) == 0

		n, err := p.fetchMessages()
		if err != nil {
			if err != internal.ErrNotSupported {
				return err
			}
		}

		if n == 0 && isIdle {
			noWork++
		} else {
			noWork = 0
		}
		if noWork == 2 {
			break
		}

		if err == internal.ErrNotSupported {
			// Don't burn CPU waiting.
			time.Sleep(100 * time.Millisecond)
		}
	}

	return p.Stop()
}

// ProcessOne processes at most one message in the queue.
func (p *Processor) ProcessOne() error {
	msg, err := p.reserveOne()
	if err != nil {
		return err
	}

	firstErr := p.process(-1, msg)
	if err := p.delBatch.Wait(); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}

func (p *Processor) reserveOne() (*Message, error) {
	select {
	case msg := <-p.ch:
		return msg, nil
	default:
	}

	msgs, err := p.q.ReserveN(1)
	if err != nil && err != internal.ErrNotSupported {
		return nil, err
	}

	if len(msgs) == 0 {
		return nil, errors.New("msgqueue: queue is empty")
	}

	atomic.AddUint32(&p.inFlight, 1)
	return &msgs[0], nil
}

func (p *Processor) startMessageFetcher() bool {
	if !atomic.CompareAndSwapUint32(&p.messageFetcherStarted, 0, 1) {
		return false
	}

	p.workersWG.Add(1)
	go p.messageFetcher()

	return true
}

func (p *Processor) messageFetcher() {
	defer p.workersWG.Done()

	for {
		if atomic.LoadUint32(&p.messageFetcherStop) == 1 {
			break
		}

		if pauseTime := p.paused(); pauseTime > 0 {
			p.resetPause()
			internal.Logf("%s is automatically paused for dur=%s", p.q, pauseTime)
			time.Sleep(pauseTime)
			continue
		}

		_, err := p.fetchMessages()
		if err != nil {
			if err == internal.ErrNotSupported {
				break
			}

			internal.Logf(
				"%s ReserveN failed: %s (sleeping for dur=%s)",
				p.q, err, p.opt.WaitTimeout,
			)
			time.Sleep(p.opt.WaitTimeout)
		}
	}
}

func (p *Processor) fetchMessages() (int, error) {
	msgs, err := p.q.ReserveN(p.opt.BufferSize)
	if err != nil {
		return 0, err
	}
	for i := range msgs {
		p.Add(&msgs[i])
	}
	return len(msgs), nil
}

func (p *Processor) worker(id int, stop <-chan struct{}) {
	defer p.workersWG.Done()

	if p.opt.WorkerLimit > 0 {
		defer p.unlockWorker(id)
	}

	for {
		if p.opt.WorkerLimit > 0 {
			p.lockWorker(id)
		}

		msg, ok := p.dequeueMessage(stop)
		if !ok {
			break
		}

		p.process(id, msg)
	}

}

func (p *Processor) process(workerId int, msg *Message) error {
	if msg.Delay > 0 {
		p.release(msg, nil)
		return nil
	}

	start := time.Now()
	err := p.handler.HandleMessage(msg)
	p.updateAvgDuration(time.Since(start))

	if err == nil {
		p.resetPause()
		atomic.AddUint32(&p.processed, 1)
		p.delete(msg, nil)
		return nil
	}

	atomic.AddUint32(&p.errCount, 1)
	if msg.ReservedCount < p.opt.RetryLimit {
		atomic.AddUint32(&p.retries, 1)
		p.release(msg, err)
	} else {
		atomic.AddUint32(&p.fails, 1)
		p.delete(msg, err)
	}

	return err
}

// Purge discards messages from the internal queue.
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

func (p *Processor) dequeueMessage(stop <-chan struct{}) (*Message, bool) {
	p.startMessageFetcher()

	if p.opt.RateLimiter != nil {
		select {
		case <-p.allowRate(stop):
		case <-stop:
			return p.dequeueMessageOrStop()
		}
	}

	select {
	case msg := <-p.ch:
		return msg, true
	case <-stop:
		return p.dequeueMessageOrStop()
	}
}

func (p *Processor) dequeueMessageOrStop() (*Message, bool) {
	select {
	case msg := <-p.ch:
		return msg, true
	default:
		return nil, false
	}
}

func (p *Processor) allowRate(stop <-chan struct{}) <-chan struct{} {
	allowCh := make(chan struct{})
	go func() {
		for {
			delay, allow := p.opt.RateLimiter.AllowRate(p.q.Name(), p.opt.RateLimit)
			if allow {
				close(allowCh)
				return
			}

			time.Sleep(delay)
			select {
			case <-stop:
				return
			default:
			}
		}
	}()
	return allowCh
}

func (p *Processor) release(msg *Message, reason error) {
	delay := p.releaseBackoff(msg, reason)

	if reason != nil {
		new := uint32(delay / time.Second)
		for {
			old := atomic.LoadUint32(&p.delaySec)
			if new > old {
				break
			}
			if atomic.CompareAndSwapUint32(&p.delaySec, old, new) {
				break
			}
		}

		internal.Logf("%s handler failed (retry in dur=%s): %s", p.q, delay, reason)
	}
	if err := p.q.Release(msg, delay); err != nil {
		internal.Logf("%s Release failed: %s", p.q, err)
	}

	atomic.AddUint32(&p.inFlight, ^uint32(0))
	p.wg.Done()
}

func (p *Processor) releaseBackoff(msg *Message, reason error) time.Duration {
	if reason != nil {
		if delayer, ok := reason.(Delayer); ok {
			return delayer.Delay()
		}
	}

	if msg.Delay > 0 {
		return msg.Delay
	}

	return exponentialBackoff(p.opt.MinBackoff, p.opt.MaxBackoff, msg.ReservedCount)
}

func (p *Processor) delete(msg *Message, reason error) {
	if reason != nil {
		internal.Logf("%s handler failed: %s", p.q, reason)

		if p.fallbackHandler != nil {
			if err := p.fallbackHandler.HandleMessage(msg); err != nil {
				internal.Logf("%s fallback handler failed: %s", p.q, err)
			}
		}
	}

	atomic.AddUint32(&p.inFlight, ^uint32(0))
	atomic.AddUint32(&p.deleting, 1)
	p.delBatch.Add(msg)
}

func (p *Processor) deleteBatch(msgs []*Message) {
	if err := p.q.DeleteBatch(msgs); err != nil {
		internal.Logf("%s DeleteBatch failed: %s", p.q, err)
	}
	atomic.AddUint32(&p.deleting, ^uint32(len(msgs)-1))
	for i := 0; i < len(msgs); i++ {
		p.wg.Done()
	}
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
	atomic.StoreUint32(&p.delaySec, 0)
	atomic.StoreUint32(&p.errCount, 0)
}

func (p *Processor) lockWorker(id int) {
	const timeout = 1234 * time.Millisecond

	lock := p.workerLocks[id]
	for {
		ok, err := lock.Lock()
		if err != nil {
			internal.Logf("redlock.Lock failed: %s", err)
		}
		if ok {
			return
		}
		time.Sleep(timeout)
	}
}

func (p *Processor) unlockWorker(id int) {
	lock := p.workerLocks[id]
	if err := lock.Unlock(); err != nil {
		internal.Logf("redlock.Unlock failed: %s", err)
	}
}

func exponentialBackoff(min, max time.Duration, retry int) time.Duration {
	dur := min << uint(retry-1)
	if dur >= min && dur < max {
		return dur
	}
	return max
}
