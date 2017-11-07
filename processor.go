package msgqueue

import (
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bsm/redis-lock"

	"github.com/go-msgqueue/msgqueue/internal"
)

const timePrecision = time.Microsecond
const stopTimeout = 30 * time.Second

type Delayer interface {
	Delay() time.Duration
}

type ProcessorStats struct {
	WorkerNumber  uint32
	FetcherNumber uint32
	BufferSize    uint32
	Buffered      uint32
	InFlight      uint32
	Processed     uint32
	Retries       uint32
	Fails         uint32
	AvgDuration   time.Duration
	MinDuration   time.Duration
	MaxDuration   time.Duration
}

// Processor reserves messages from the queue, processes them,
// and then either releases or deletes messages from the queue.
type Processor struct {
	q   Queue
	opt *Options

	handler         Handler
	fallbackHandler Handler

	buffer chan *Message

	stopCh chan struct{}

	workerNumber  uint32 // atomic
	workerLocks   []*lock.Lock
	fetcherNumber uint32 // atomic

	jobsWG sync.WaitGroup

	rateLimitAllowed   uint32
	rateLimitAllowance uint32

	errCount uint32
	delaySec uint32

	inFlight    uint32
	deleting    uint32
	processed   uint32
	fails       uint32
	retries     uint32
	avgDuration uint32
	minDuration uint32
	maxDuration uint32
}

// New creates new Processor for the queue using provided processing options.
func NewProcessor(q Queue, opt *Options) *Processor {
	if opt.Name == "" {
		opt.Name = q.Name()
	}
	opt.Init()

	p := &Processor{
		q:   q,
		opt: opt,

		buffer: make(chan *Message, opt.BufferSize),
	}

	p.handler = NewHandler(opt.Handler)
	if opt.FallbackHandler != nil {
		p.fallbackHandler = NewHandler(opt.FallbackHandler)
	}

	return p
}

// Starts creates new Processor and starts it.
func StartProcessor(q Queue, opt *Options) *Processor {
	p := NewProcessor(q, opt)
	p.Start()
	return p
}

func (p *Processor) Queue() Queue {
	return p.q
}

func (p *Processor) Options() *Options {
	return p.opt
}

func (p *Processor) String() string {
	return fmt.Sprintf("Processor<%s>", p.q.Name())
}

// Stats returns processor stats.
func (p *Processor) Stats() *ProcessorStats {
	return &ProcessorStats{
		WorkerNumber:  atomic.LoadUint32(&p.workerNumber),
		FetcherNumber: atomic.LoadUint32(&p.fetcherNumber),
		BufferSize:    uint32(cap(p.buffer)),
		Buffered:      uint32(len(p.buffer)),
		InFlight:      atomic.LoadUint32(&p.inFlight),
		Processed:     atomic.LoadUint32(&p.processed),
		Retries:       atomic.LoadUint32(&p.retries),
		Fails:         atomic.LoadUint32(&p.fails),
		AvgDuration:   time.Duration(atomic.LoadUint32(&p.avgDuration)) * timePrecision,
		MinDuration:   time.Duration(atomic.LoadUint32(&p.minDuration)) * timePrecision,
		MaxDuration:   time.Duration(atomic.LoadUint32(&p.maxDuration)) * timePrecision,
	}
}

func (p *Processor) Add(msg *Message) error {
	if msg.Delay > 0 {
		time.AfterFunc(msg.Delay, func() {
			msg.Delay = 0
			p.add(msg)
		})
	} else {
		p.add(msg)
	}
	return nil
}

func (p *Processor) Len() int {
	return len(p.buffer)
}

func (p *Processor) add(msg *Message) {
	_ = p.reservationSize(1)
	p.buffer <- msg
}

// Process is low-level API to process message bypassing the internal queue.
func (p *Processor) Process(msg *Message) error {
	return p.process(msg)
}

// Start starts processing messages in the queue.
func (p *Processor) Start() error {
	if p.stopCh != nil {
		return errors.New("Processor is already started")
	}

	stop := make(chan struct{})
	p.stopCh = stop

	p.addWorker(stop)
	p.addFetcher(stop)

	p.jobsWG.Add(1)
	go p.autotune(stop)

	return nil
}

func (p *Processor) addWorker(stop <-chan struct{}) {
	id := atomic.AddUint32(&p.workerNumber, 1) - 1
	if id >= uint32(p.opt.MaxWorkers) {
		atomic.AddUint32(&p.workerNumber, ^uint32(0))
		return
	}
	if p.opt.WorkerLimit > 0 {
		key := fmt.Sprintf("%s:worker-lock:%d", p.q.Name(), id)
		workerLock := lock.NewLock(p.opt.Redis, key, &lock.LockOptions{
			LockTimeout: p.opt.ReservationTimeout,
		})
		p.workerLocks = append(p.workerLocks, workerLock)
	}
	p.startWorker(id, stop)
}

func (p *Processor) startWorker(id uint32, stop <-chan struct{}) {
	p.jobsWG.Add(1)
	go p.worker(id, stop)
}

func (p *Processor) addFetcher(stop <-chan struct{}) {
	id := atomic.AddUint32(&p.fetcherNumber, 1) - 1
	if id >= uint32(p.opt.MaxFetchers) {
		atomic.AddUint32(&p.fetcherNumber, ^uint32(0))
		return
	}
	p.startFetcher(id, stop)
}

func (p *Processor) startFetcher(id uint32, stop <-chan struct{}) {
	p.jobsWG.Add(1)
	go p.fetcher(id, stop)
}

func (p *Processor) removeFetcher() {
	atomic.AddUint32(&p.fetcherNumber, ^uint32(0))
}

func (p *Processor) autotune(stop <-chan struct{}) {
	defer p.jobsWG.Done()

	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()

	for {
		select {
		case <-stop:
			return
		case <-timer.C:
			p._autotune(stop)
		}
	}
}

func (p *Processor) _autotune(stop <-chan struct{}) {
	n, _ := p.q.Len()
	queueing := n > 256

	if len(p.buffer) == 0 && queueing {
		if atomic.LoadUint32(&p.rateLimitAllowed) >= 3 {
			p.addFetcher(stop)
		}
		return
	}

	if cap(p.buffer)-len(p.buffer) < cap(p.buffer)/2 || queueing {
		for i := 0; i < 3; i++ {
			p.addWorker(stop)
		}
	}
}

// Stop is StopTimeout with 30 seconds timeout.
func (p *Processor) Stop() error {
	return p.StopTimeout(stopTimeout)
}

// StopTimeout waits workers for timeout duration to finish processing current
// messages and stops workers.
func (p *Processor) StopTimeout(timeout time.Duration) error {
	if p.stopCh == nil || closed(p.stopCh) {
		return nil
	}
	close(p.stopCh)
	p.stopCh = nil

	atomic.StoreUint32(&p.fetcherNumber, 0)
	atomic.StoreUint32(&p.workerNumber, 0)

	done := make(chan struct{}, 1)
	timeoutCh := time.After(timeout)

	go func() {
		p.jobsWG.Wait()
		done <- struct{}{}
	}()

	select {
	case <-done:
	case <-timeoutCh:
		return fmt.Errorf("workers are not stopped after %s", timeout)
	}

	return nil
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
	p.Start()

	var prev *ProcessorStats
	var noWork int
	for {
		st := p.Stats()
		if prev != nil &&
			st.Buffered == 0 &&
			st.InFlight == 0 &&
			st.Processed == prev.Processed {
			noWork++
			if noWork == 2 {
				break
			}
		} else {
			noWork = 0
		}
		prev = st
		time.Sleep(time.Second)
	}

	return p.Stop()
}

// ProcessOne processes at most one message in the queue.
func (p *Processor) ProcessOne() error {
	msg, err := p.reserveOne()
	if err != nil {
		return err
	}

	// TODO: wait
	return p.process(msg)
}

func (p *Processor) reserveOne() (*Message, error) {
	select {
	case msg := <-p.buffer:
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
	if len(msgs) != 1 {
		return nil, fmt.Errorf("msgqueue: queue returned %d messages", len(msgs))
	}

	return msgs[0], nil
}

func (p *Processor) fetcher(id uint32, stop <-chan struct{}) {
	defer p.jobsWG.Done()
	for {
		if closed(stop) {
			break
		}

		if id > atomic.LoadUint32(&p.fetcherNumber) {
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
				"%s fetchMessages failed: %s (sleeping for dur=%s)",
				p.q, err, p.opt.WaitTimeout,
			)
			time.Sleep(p.opt.WaitTimeout)
		}
	}
}

func (p *Processor) fetchMessages() (int, error) {
	size := p.reservationSize(p.opt.ReservationSize)
	msgs, err := p.q.ReserveN(size)
	if err != nil {
		return 0, err
	}

	if d := size - len(msgs); d > 0 {
		p.saveReservationSize(d)
	}

	timeout := make(chan struct{})
	timer := time.AfterFunc(p.opt.ReservationTimeout*4/5, func() {
		close(timeout)
	})
	defer timer.Stop()

	for _, msg := range msgs {
		select {
		case p.buffer <- msg:
		case <-timeout:
			p.release(msg, nil)
		}
	}

	select {
	case <-timeout:
		p.removeFetcher()
		p.releaseBuffer()
	default:
	}

	return len(msgs), nil
}

func (p *Processor) reservationSize(max int) int {
	if p.opt.RateLimiter == nil {
		return max
	}

	allowance := atomic.LoadUint32(&p.rateLimitAllowance)
	if allowance > 0 {
		if allowance >= uint32(max) {
			atomic.AddUint32(&p.rateLimitAllowance, ^uint32(max-1))
			return max
		}

		atomic.AddUint32(&p.rateLimitAllowance, ^uint32(allowance-1))
		return int(allowance)
	}

	var size int
	for {
		delay, allow := p.opt.RateLimiter.AllowRate(p.q.Name(), p.opt.RateLimit)
		if allow {
			size++
			if size == max {
				atomic.AddUint32(&p.rateLimitAllowed, 1)
				return size
			}
			continue
		} else {
			atomic.StoreUint32(&p.rateLimitAllowed, 0)
		}

		if size > 0 {
			return size
		}
		time.Sleep(delay)
	}
}

func (p *Processor) saveReservationSize(n int) {
	if p.opt.RateLimiter == nil {
		return
	}
	atomic.AddUint32(&p.rateLimitAllowance, uint32(n))
}

func (p *Processor) releaseBuffer() {
	for {
		msg, ok := p.dequeueMessage()
		if !ok {
			return
		}
		p.release(msg, nil)
	}
}

func (p *Processor) worker(id uint32, stop <-chan struct{}) {
	defer p.jobsWG.Done()

	if p.opt.WorkerLimit > 0 {
		defer p.unlockWorker(id)
	}

	for {
		if id > atomic.LoadUint32(&p.workerNumber) {
			break
		}

		if p.opt.WorkerLimit > 0 {
			if !p.lockWorker(id, stop) {
				break
			}
		}

		msg, ok := p.waitMessageOrStop(stop)
		if !ok {
			break
		}

		select {
		case <-stop:
			p.release(msg, nil)
		default:
			p.process(msg)
		}
	}
}

func (p *Processor) process(msg *Message) error {
	atomic.AddUint32(&p.inFlight, 1)

	if msg.Delay > 0 {
		p.release(msg, nil)
		return nil
	}
	msg.Delay = exponentialBackoff(p.opt.MinBackoff, p.opt.MaxBackoff, msg.ReservedCount)

	start := time.Now()
	err := p.handler.HandleMessage(msg)
	if err == errBatched {
		return nil
	}
	p.updateAvgDuration(time.Since(start))
	if err == errBatchProcessed {
		return nil
	}

	if err == nil {
		p.resetPause()
	}
	p.put(msg, err)

	return err
}

func (p *Processor) put(msg *Message, err error) {
	if err == nil {
		atomic.AddUint32(&p.processed, 1)
		p.delete(msg, err)
		return
	}

	atomic.AddUint32(&p.errCount, 1)
	if msg.ReservedCount < p.opt.RetryLimit {
		atomic.AddUint32(&p.retries, 1)
		p.release(msg, err)
	} else {
		atomic.AddUint32(&p.fails, 1)
		p.delete(msg, err)
	}
}

func (p *Processor) Put(msg *Message) error {
	p.put(msg, msg.Err)
	return nil
}

// Purge discards messages from the internal queue.
func (p *Processor) Purge() error {
	for {
		select {
		case msg := <-p.buffer:
			p.delete(msg, nil)
		default:
			return nil
		}
	}
}

func (p *Processor) waitMessageOrStop(stop <-chan struct{}) (*Message, bool) {
	msg, ok := p.dequeueMessage()
	if ok {
		return msg, true
	}

	if atomic.LoadUint32(&p.fetcherNumber) == 0 {
		p.addFetcher(stop)
	}

	select {
	case msg := <-p.buffer:
		return msg, true
	case <-stop:
		return p.dequeueMessage()
	}
}

func (p *Processor) dequeueMessage() (*Message, bool) {
	select {
	case msg := <-p.buffer:
		return msg, true
	default:
		return nil, false
	}
}

func (p *Processor) release(msg *Message, err error) {
	msg.Delay = p.releaseBackoff(msg, err)

	if err != nil {
		new := uint32(msg.Delay / time.Second)
		for new > 0 {
			old := atomic.LoadUint32(&p.delaySec)
			if new > old {
				break
			}
			if atomic.CompareAndSwapUint32(&p.delaySec, old, new) {
				break
			}
		}

		internal.Logf(
			"%s handler failed (retry in dur=%s): %s",
			p.q, msg.Delay, err,
		)
	}

	if err := p.q.Release(msg); err != nil {
		internal.Logf("%s Release failed: %s", p.q, err)
	}
	atomic.AddUint32(&p.inFlight, ^uint32(0))
}

func (p *Processor) releaseBackoff(msg *Message, err error) time.Duration {
	if err != nil {
		if delayer, ok := err.(Delayer); ok {
			return delayer.Delay()
		}
	}
	return msg.Delay
}

func (p *Processor) delete(msg *Message, err error) {
	if err != nil {
		internal.Logf("%s handler failed: %s", p.q, err)

		if p.fallbackHandler != nil {
			if err := p.fallbackHandler.HandleMessage(msg); err != nil {
				internal.Logf("%s fallback handler failed: %s", p.q, err)
			}
		}
	}

	p.q.Delete(msg)
	atomic.AddUint32(&p.inFlight, ^uint32(0))
}

func (p *Processor) updateAvgDuration(dur time.Duration) {
	const decay = float32(1) / 30

	us := uint32(dur / timePrecision)
	if us == 0 {
		return
	}

	for {
		min := atomic.LoadUint32(&p.minDuration)
		if (min != 0 && us >= min) ||
			atomic.CompareAndSwapUint32(&p.minDuration, min, us) {
			break
		}
	}

	for {
		max := atomic.LoadUint32(&p.maxDuration)
		if us <= max || atomic.CompareAndSwapUint32(&p.maxDuration, max, us) {
			break
		}
	}

	for {
		avg := atomic.LoadUint32(&p.avgDuration)
		var newAvg uint32
		if avg > 0 {
			newAvg = uint32((1-decay)*float32(avg) + decay*float32(us))
		} else {
			newAvg = us
		}
		if atomic.CompareAndSwapUint32(&p.avgDuration, avg, newAvg) {
			break
		}
	}
}

func (p *Processor) resetPause() {
	atomic.StoreUint32(&p.delaySec, 0)
	atomic.StoreUint32(&p.errCount, 0)
}

func (p *Processor) lockWorker(id uint32, stop <-chan struct{}) bool {
	lock := p.workerLocks[id]
	for {
		ok, err := lock.Lock()
		if err != nil {
			internal.Logf("redlock.Lock failed: %s", err)
		}
		if ok {
			return true
		}

		sleep := time.Duration(rand.Intn(1000)) * time.Millisecond
		select {
		case <-stop:
			return false
		case <-time.After(sleep):
		}
	}
}

func (p *Processor) unlockWorker(id uint32) {
	lock := p.workerLocks[id]
	if err := lock.Unlock(); err != nil {
		internal.Logf("redlock.Unlock failed: %s", err)
	}
}

func closed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

func exponentialBackoff(min, max time.Duration, retry int) time.Duration {
	dur := min << uint(retry-1)
	if dur >= min && dur < max {
		return dur
	}
	return max
}
