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

const timePrecision = 100 * time.Microsecond
const stopTimeout = 30 * time.Second

const (
	stateReady   = 0
	stateStarted = 1
	stateStopped = -1
)

func startWorker(state *int32) bool {
	return atomic.CompareAndSwapInt32(state, stateReady, stateStarted)
}

func stopWorker(state *int32) bool {
	for {
		if atomic.CompareAndSwapInt32(state, stateStarted, stateStopped) {
			return true
		}

		if atomic.LoadInt32(state) == stateStopped {
			return false
		}

		if atomic.CompareAndSwapInt32(state, stateReady, stateStopped) {
			return false
		}
	}
}

func resetWorker(state *int32) bool {
	return atomic.CompareAndSwapInt32(state, stateStopped, stateReady)
}

func workerStopped(state *int32) bool {
	return atomic.LoadInt32(state) != stateStarted
}

type Delayer interface {
	Delay() time.Duration
}

type ProcessorStats struct {
	Buffered    uint32
	InFlight    uint32
	Processed   uint32
	Retries     uint32
	Fails       uint32
	AvgDuration time.Duration
	MinDuration time.Duration
	MaxDuration time.Duration
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

	workersWG    sync.WaitGroup
	workersState int32
	workersStop  chan struct{}
	workerLocks  []*lock.Lock

	messageFetcherWG    sync.WaitGroup
	messageFetcherState int32

	rateLimitAllowance int

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
	return fmt.Sprintf(
		"Processor<%s workers=%d buffer=%d>",
		p.q.Name(), p.opt.WorkerNumber, p.opt.BufferSize,
	)
}

// Stats returns processor stats.
func (p *Processor) Stats() *ProcessorStats {
	return &ProcessorStats{
		Buffered:    uint32(len(p.ch)),
		InFlight:    atomic.LoadUint32(&p.inFlight),
		Processed:   atomic.LoadUint32(&p.processed),
		Retries:     atomic.LoadUint32(&p.retries),
		Fails:       atomic.LoadUint32(&p.fails),
		AvgDuration: time.Duration(atomic.LoadUint32(&p.avgDuration)) * timePrecision,
		MinDuration: time.Duration(atomic.LoadUint32(&p.minDuration)) * timePrecision,
		MaxDuration: time.Duration(atomic.LoadUint32(&p.maxDuration)) * timePrecision,
	}
}

func (p *Processor) Add(msg *Message) error {
	p.wg.Add(1)
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

func (p *Processor) add(msg *Message) {
	_ = p.reservationSize(1)
	p.ch <- msg
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
	if !startWorker(&p.workersState) {
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
	if !stopWorker(&p.workersState) {
		return nil
	}
	defer resetWorker(&p.workersState)

	p.stopMessageFetcher()
	defer p.resetMessageFetcher()

	done := make(chan struct{}, 1)
	timeoutCh := time.After(timeout)

	go func() {
		p.messageFetcherWG.Wait()
		done <- struct{}{}
	}()

	select {
	case <-done:
		p.releaseBuffer()
	case <-timeoutCh:
		return fmt.Errorf("message fetcher is not stopped after %s", timeout)
	}

	go func() {
		p.wg.Wait()
		done <- struct{}{}
	}()

	select {
	case <-done:
	case <-timeoutCh:
		return fmt.Errorf("messages are not processed after %s", timeout)
	}

	close(p.workersStop)
	p.workersStop = nil

	go func() {
		p.workersWG.Wait()
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
	p.startWorkers()

	var noWork int
	for {
		isIdle := len(p.ch) == 0 && atomic.LoadUint32(&p.inFlight) == 0

		n, err := p.fetchMessages()
		if err != nil && err != internal.ErrNotSupported {
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

	// TODO: wait
	return p.process(-1, msg)
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
	if len(msgs) != 1 {
		return nil, fmt.Errorf("msgqueue: queue returned %d messages", len(msgs))
	}

	p.wg.Add(1)
	return msgs[0], nil
}

func (p *Processor) startMessageFetcher() {
	if startWorker(&p.messageFetcherState) {
		p.messageFetcherWG.Add(1)
		go p.messageFetcher()
	}
}

func (p *Processor) stopMessageFetcher() {
	stopWorker(&p.messageFetcherState)
}

func (p *Processor) resetMessageFetcher() {
	if !resetWorker(&p.messageFetcherState) {
		panic("unreached")
	}
}

func (p *Processor) messageFetcher() {
	defer p.messageFetcherWG.Done()
	for {
		if workerStopped(&p.messageFetcherState) {
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
		p.wg.Add(1)
		select {
		case p.ch <- msg:
		case <-timeout:
			p.release(msg, nil)
		}
	}

	select {
	case <-timeout:
		internal.Logf("%s is stuck - stopping message fetcher", p.q)
		p.stopMessageFetcher()
		p.resetMessageFetcher()
		p.releaseBuffer()
	default:
	}

	return len(msgs), nil
}

func (p *Processor) reservationSize(max int) int {
	if p.opt.RateLimiter == nil {
		return max
	}

	if p.rateLimitAllowance > 0 {
		if max < p.rateLimitAllowance {
			p.rateLimitAllowance -= max
			return max
		}

		size := p.rateLimitAllowance
		p.rateLimitAllowance = 0
		return size
	}

	var size int
	for {
		delay, allow := p.opt.RateLimiter.AllowRate(p.q.Name(), p.opt.RateLimit)
		if allow {
			size++
			if size >= max {
				return size
			}
			continue
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
	p.rateLimitAllowance = n
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

func (p *Processor) worker(id int, stop <-chan struct{}) {
	defer p.workersWG.Done()

	if p.opt.WorkerLimit > 0 {
		defer p.unlockWorker(id)
	}

	for {
		if p.opt.WorkerLimit > 0 {
			if !p.lockWorker(id, stop) {
				return
			}
		}

		msg, ok := p.waitMessageOrStop(stop)
		if !ok {
			return
		}

		p.process(id, msg)
	}
}

func (p *Processor) process(workerId int, msg *Message) error {
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
		case msg := <-p.ch:
			p.delete(msg, nil)
		default:
			return nil
		}
	}
}

func (p *Processor) waitMessageOrStop(stop <-chan struct{}) (*Message, bool) {
	p.startMessageFetcher()

	select {
	case msg := <-p.ch:
		return msg, true
	case <-stop:
		return p.dequeueMessage()
	}
}

func (p *Processor) dequeueMessage() (*Message, bool) {
	select {
	case msg := <-p.ch:
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
	p.wg.Done()
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
	p.wg.Done()
}

func (p *Processor) updateAvgDuration(dur time.Duration) {
	const decay = float32(1) / 30

	us := uint32(dur / timePrecision)
	if us == 0 {
		return
	}

	for {
		min := atomic.LoadUint32(&p.minDuration)
		if (min != 0 && us >= min) || atomic.CompareAndSwapUint32(&p.minDuration, min, us) {
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

func (p *Processor) lockWorker(id int, stop <-chan struct{}) bool {
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

func (p *Processor) unlockWorker(id int) {
	lock := p.workerLocks[id]
	if err := lock.Unlock(); err != nil {
		internal.Logf("redlock.Unlock failed: %s", err)
	}
}

func (p *Processor) splitDeleteBatch(msgs []*Message) ([]*Message, []*Message) {
	if len(msgs) >= p.opt.ReservationSize {
		return msgs, nil
	}
	return nil, msgs
}

func exponentialBackoff(min, max time.Duration, retry int) time.Duration {
	dur := min << uint(retry-1)
	if dur >= min && dur < max {
		return dur
	}
	return max
}
