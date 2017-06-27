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
	Deleting    uint32
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

	workersWG   sync.WaitGroup
	workerLocks []*lock.Lock

	workersState int32
	workersStop  chan struct{}

	// 0 - ready
	// 1 - started
	// -1 - stopped
	messageFetcherState int32
	rateLimitAllowance  int

	delBatch *msgBatcher

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
		Deleting:    atomic.LoadUint32(&p.deleting),
		Processed:   atomic.LoadUint32(&p.processed),
		Retries:     atomic.LoadUint32(&p.retries),
		Fails:       atomic.LoadUint32(&p.fails),
		AvgDuration: time.Duration(atomic.LoadUint32(&p.avgDuration)) * time.Microsecond,
		MinDuration: time.Duration(atomic.LoadUint32(&p.minDuration)) * time.Microsecond,
		MaxDuration: time.Duration(atomic.LoadUint32(&p.maxDuration)) * time.Microsecond,
	}
}

func (p *Processor) setHandler(handler interface{}) {
	p.handler = NewHandler(handler)
}

func (p *Processor) setFallbackHandler(handler interface{}) {
	p.fallbackHandler = NewHandler(handler)
}

func (p *Processor) inc() {
	p.wg.Add(1)
	atomic.AddUint32(&p.inFlight, 1)
}

func (p *Processor) dec() {
	atomic.AddUint32(&p.inFlight, ^uint32(0))
	p.wg.Done()
}

// Process is low-level API to process message bypassing the internal queue.
func (p *Processor) Process(msg *Message) error {
	p.inc()
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

	p.workersWG.Add(1)
	go func() {
		p.releaseBuffer()
		p.workersWG.Done()
	}()

	p.delBatch.SetLimit(1)
	defer p.delBatch.SetLimit(p.opt.BufferSize)
	p.delBatch.Wait()

	done := make(chan struct{}, 1)
	timeoutCh := time.After(timeout)

	go func() {
		p.wg.Wait()
		done <- struct{}{}
	}()

	select {
	case <-timeoutCh:
		return fmt.Errorf("messages were not processed after %s", timeout)
	case <-done:
	}

	close(p.workersStop)
	p.workersStop = nil

	go func() {
		p.workersWG.Wait()
		done <- struct{}{}
	}()

	select {
	case <-timeoutCh:
		return fmt.Errorf("workers were not stopped after %s", timeout)
	case <-done:
	}

	go func() {
		p.delBatch.Wait()
		done <- struct{}{}
	}()

	select {
	case <-timeoutCh:
		return fmt.Errorf("messages were not deleted after %s", timeout)
	case <-done:
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
	if err != nil {
		return nil, err
	}

	if len(msgs) == 0 {
		return nil, errors.New("msgqueue: queue is empty")
	}
	if len(msgs) != 1 {
		return nil, fmt.Errorf("msgqueue: queue returned %d messages", len(msgs))
	}

	p.inc()
	return msgs[0], nil
}

func (p *Processor) startMessageFetcher() {
	if startWorker(&p.messageFetcherState) {
		p.workersWG.Add(1)
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
	defer p.workersWG.Done()

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
			internal.Logf(
				"%s ReserveN failed: %s (sleeping for dur=%s)",
				p.q, err, p.opt.WaitTimeout,
			)
			time.Sleep(p.opt.WaitTimeout)
		}
	}
}

func (p *Processor) fetchMessages() (int, error) {
	size := p.reservationSize()
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
		p.inc()
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

func (p *Processor) reservationSize() int {
	if p.opt.RateLimiter == nil {
		return p.opt.BufferSize
	}

	if p.rateLimitAllowance > 0 {
		size := p.rateLimitAllowance
		p.rateLimitAllowance = 0
		return size
	}

	var size int
	for {
		delay, allow := p.opt.RateLimiter.AllowRate(p.q.Name(), p.opt.RateLimit)
		if allow {
			size++
			if size > p.opt.BufferSize {
				break
			}
			continue
		}
		if size > 0 {
			break
		}
		time.Sleep(delay)
	}

	return size
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

func (p *Processor) release(msg *Message, reason error) {
	delay := p.releaseBackoff(msg, reason)

	if reason != nil {
		new := uint32(delay / time.Second)
		for new > 0 {
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

	p.dec()
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

	atomic.AddUint32(&p.deleting, 1)
	atomic.AddUint32(&p.inFlight, ^uint32(0))
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
	const decay = float32(1) / 100

	us := uint32(dur / time.Microsecond)

	for {
		avg := atomic.LoadUint32(&p.avgDuration)
		newAvg := uint32((1-decay)*float32(avg) + decay*float32(us))
		if atomic.CompareAndSwapUint32(&p.avgDuration, avg, newAvg) {
			break
		}
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

func exponentialBackoff(min, max time.Duration, retry int) time.Duration {
	dur := min << uint(retry-1)
	if dur >= min && dur < max {
		return dur
	}
	return max
}
