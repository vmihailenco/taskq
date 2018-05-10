package msgqueue

import (
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bsm/redis-lock"
	"golang.org/x/time/rate"

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

type limiter struct {
	bucket  string
	limiter RateLimiter
	limit   rate.Limit

	allowedCount uint32 // atomic
	cancelled    uint32 // atomic
}

func (l *limiter) Reserve(max int) int {
	if l.limiter == nil {
		return max
	}

	for {
		cancelled := atomic.LoadUint32(&l.cancelled)
		if cancelled == 0 {
			break
		}

		if cancelled >= uint32(max) {
			if atomic.CompareAndSwapUint32(&l.cancelled, cancelled, uint32(max)-1) {
				return max
			}
			continue
		}

		if atomic.CompareAndSwapUint32(&l.cancelled, cancelled, uint32(cancelled)-1) {
			return int(cancelled)
		}
	}

	var size int
	for {
		delay, allow := l.limiter.AllowRate(l.bucket, l.limit)
		if allow {
			size++
			if size == max {
				atomic.AddUint32(&l.allowedCount, 1)
				return size
			}
			continue
		} else {
			atomic.StoreUint32(&l.allowedCount, 0)
		}

		if size > 0 {
			return size
		}
		time.Sleep(delay)
	}
}

func (l *limiter) Cancel(n int) {
	if l.limiter == nil {
		return
	}
	atomic.AddUint32(&l.cancelled, uint32(n))
}

func (l *limiter) Limited() bool {
	return l.limiter != nil && atomic.LoadUint32(&l.allowedCount) < 3
}

// Processor reserves messages from the queue, processes them,
// and then either releases or deletes messages from the queue.
type Processor struct {
	q   Queue
	opt *Options

	handler         Handler
	fallbackHandler Handler

	buffer  chan *Message
	limiter *limiter

	stopCh chan struct{}

	workerNumber  int32 // atomic
	workerLocks   []*lock.Locker
	fetcherNumber int32 // atomic

	jobsWG sync.WaitGroup

	queueLen    int
	queueing    int
	bufferEmpty int
	fetcherIdle uint32 // atomic
	workerIdle  uint32 // atomic

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
		limiter: &limiter{
			bucket:  q.Name(),
			limiter: opt.RateLimiter,
			limit:   opt.RateLimit,
		},
	}

	p.handler = NewHandler(opt.Handler, opt.Compress)
	if opt.FallbackHandler != nil {
		p.fallbackHandler = NewHandler(opt.FallbackHandler, opt.Compress)
	}

	return p
}

// Starts creates new Processor and starts it.
func StartProcessor(q Queue, opt *Options) *Processor {
	p := NewProcessor(q, opt)
	if err := p.Start(); err != nil {
		panic(err)
	}
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
		WorkerNumber:  uint32(atomic.LoadInt32(&p.workerNumber)),
		FetcherNumber: uint32(atomic.LoadInt32(&p.fetcherNumber)),
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
	_ = p.limiter.Reserve(1)
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

	atomic.StoreInt32(&p.fetcherNumber, 0)
	atomic.StoreInt32(&p.workerNumber, 0)

	p.addWorker(stop)

	p.jobsWG.Add(1)
	go p.autotune(stop)

	return nil
}

func (p *Processor) addWorker(stop <-chan struct{}) int32 {
	id := atomic.AddInt32(&p.workerNumber, 1) - 1
	if id >= int32(p.opt.MaxWorkers) {
		atomic.AddInt32(&p.workerNumber, -1)
		return -1
	}

	if p.opt.WorkerLimit > 0 {
		key := fmt.Sprintf("%s:worker:%d:lock", p.q.Name(), id)
		workerLock := lock.New(p.opt.Redis, key, &lock.Options{
			LockTimeout: p.opt.ReservationTimeout,
		})
		p.workerLocks = append(p.workerLocks, workerLock)
	}
	p.startWorker(id, stop)

	return id
}

func (p *Processor) startWorker(id int32, stop <-chan struct{}) {
	p.jobsWG.Add(1)
	go p.worker(id, stop)
}

func (p *Processor) removeWorker() {
	atomic.AddInt32(&p.workerNumber, -1)
}

func (p *Processor) addFetcher(stop <-chan struct{}) int32 {
	id := atomic.AddInt32(&p.fetcherNumber, 1) - 1
	if id >= int32(p.opt.MaxFetchers) {
		atomic.AddInt32(&p.fetcherNumber, -1)
		return -1
	}

	p.startFetcher(id, stop)

	return id
}

func (p *Processor) startFetcher(id int32, stop <-chan struct{}) {
	p.jobsWG.Add(1)
	go p.fetcher(id, stop)
}

func (p *Processor) removeFetcher() {
	atomic.AddInt32(&p.fetcherNumber, -1)
}

func (p *Processor) autotune(stop <-chan struct{}) {
	defer p.jobsWG.Done()

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			p._autotune(stop)
		}
	}
}

func (p *Processor) _autotune(stop <-chan struct{}) {
	queueLen, err := p.q.Len()
	if err != nil {
		internal.Logf("%s Len failed: %s", p.q, err)
	}

	var queueing bool
	if queueLen > 256 && queueLen > p.queueLen {
		p.queueing++
		queueing = p.queueing >= 3
	} else {
		p.queueing = 0
	}
	p.queueLen = queueLen

	buffered := len(p.buffer)
	rateLimited := p.limiter.Limited()

	if buffered == 0 {
		p.bufferEmpty++
		if queueing && !rateLimited && p.bufferEmpty >= 2 && p.hasFetcher() {
			p.addFetcher(stop)
			p.queueing = 0
			p.bufferEmpty = 0
			return
		}
	} else {
		p.bufferEmpty = 0
	}

	if !queueing && atomic.LoadUint32(&p.fetcherIdle) >= 3 {
		p.removeFetcher()
		atomic.StoreUint32(&p.fetcherIdle, 0)
	}

	if (queueing && !rateLimited) || buffered > cap(p.buffer)/2 {
		for i := 0; i < 3; i++ {
			p.addWorker(stop)
		}
		p.queueing = 0
		return
	}

	if !queueing && atomic.LoadUint32(&p.workerIdle) >= 3 {
		p.removeWorker()
		atomic.StoreUint32(&p.workerIdle, 0)
	}
}

func (p *Processor) hasFetcher() bool {
	return atomic.LoadInt32(&p.fetcherNumber) > 0
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

	done := make(chan struct{}, 1)
	go func() {
		p.jobsWG.Wait()
		done <- struct{}{}
	}()

	timer := time.NewTimer(timeout)
	var err error
	select {
	case <-done:
		timer.Stop()
	case <-timer.C:
		err = fmt.Errorf("workers are not stopped after %s", timeout)
	}

	return err
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
	if err := p.Start(); err != nil {
		return err
	}

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

func (p *Processor) fetcher(id int32, stop <-chan struct{}) {
	defer p.jobsWG.Done()

	timer := time.NewTimer(time.Minute)
	timer.Stop()

	for {
		if closed(stop) {
			break
		}

		if id >= atomic.LoadInt32(&p.fetcherNumber) {
			break
		}

		if pauseTime := p.paused(); pauseTime > 0 {
			p.resetPause()
			internal.Logf("%s is automatically paused for dur=%s", p.q, pauseTime)
			time.Sleep(pauseTime)
			continue
		}

		timer.Reset(p.opt.ReservationTimeout * 4 / 5)
		timeout, err := p.fetchMessages(id, timer.C)
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
		if timeout {
			break
		}

		if !timer.Stop() {
			<-timer.C
		}
	}
}

func (p *Processor) fetchMessages(
	id int32, timeoutC <-chan time.Time,
) (timeout bool, err error) {
	size := p.limiter.Reserve(p.opt.ReservationSize)
	msgs, err := p.q.ReserveN(size)
	if err != nil {
		return false, err
	}

	if d := size - len(msgs); d > 0 {
		p.limiter.Cancel(d)
	}

	if id > 0 {
		if len(msgs) < size {
			atomic.AddUint32(&p.fetcherIdle, 1)
		} else {
			atomic.StoreUint32(&p.fetcherIdle, 0)
		}
	}

	for i, msg := range msgs {
		select {
		case p.buffer <- msg:
		case <-timeoutC:
			for _, msg := range msgs[i:] {
				p.release(msg, nil)
			}
			return true, nil
		}
	}

	return false, nil
}

func (p *Processor) releaseBuffer() {
	for {
		msg := p.dequeueMessage()
		if msg == nil {
			break
		}
		p.release(msg, nil)
	}
}

func (p *Processor) worker(id int32, stop <-chan struct{}) {
	defer p.jobsWG.Done()

	var timer *time.Timer
	var timeout <-chan time.Time
	if id > 0 {
		timer = time.NewTimer(time.Minute)
		timer.Stop()
		timeout = timer.C
	}

	if p.opt.WorkerLimit > 0 {
		defer p.unlockWorker(id)
	}

	for {
		if id >= atomic.LoadInt32(&p.workerNumber) {
			return
		}

		if p.opt.WorkerLimit > 0 {
			if !p.lockWorker(id, stop) {
				return
			}
		}

		if timer != nil {
			timer.Reset(time.Second)
		}

		msg, timeout := p.waitMessage(stop, timeout)
		if timeout {
			atomic.AddUint32(&p.workerIdle, 1)
			continue
		}

		if timer != nil {
			if !timer.Stop() {
				<-timer.C
			}
		}

		if msg == nil {
			return
		}

		select {
		case <-stop:
			p.release(msg, nil)
			return
		default:
			_ = p.process(msg)
		}
	}
}

func (p *Processor) waitMessage(
	stop <-chan struct{}, timeoutC <-chan time.Time,
) (msg *Message, timeout bool) {
	msg = p.dequeueMessage()
	if msg != nil {
		return msg, false
	}

	if !p.hasFetcher() {
		fetcherId := p.addFetcher(stop)
		if fetcherId != 0 {
			p.removeFetcher()
		}
	}

	select {
	case msg := <-p.buffer:
		return msg, false
	case <-stop:
		return p.dequeueMessage(), false
	case <-timeoutC:
		return nil, true
	}
}

func (p *Processor) dequeueMessage() *Message {
	select {
	case msg := <-p.buffer:
		return msg
	default:
		return nil
	}
}

func (p *Processor) process(msg *Message) error {
	atomic.AddUint32(&p.inFlight, 1)

	if msg.Delay > 0 {
		err := p.q.Add(msg)
		if err != nil {
			return err
		}
		p.delete(msg, nil)
		return nil
	}

	msg.Delay = exponentialBackoff(
		p.opt.MinBackoff, p.opt.MaxBackoff, msg.ReservedCount)

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

func (p *Processor) Put(msg *Message) {
	p.put(msg, msg.Err)
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

	if err := p.q.Delete(msg); err != nil {
		internal.Logf("%s Delete failed: %s", p.q, err)
	}
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

func (p *Processor) lockWorker(id int32, stop <-chan struct{}) bool {
	timer := time.NewTimer(time.Minute)
	timer.Stop()

	lock := p.workerLocks[id]
	for {
		ok, err := lock.Lock()
		if err != nil {
			internal.Logf("redlock.Lock failed: %s", err)
		}
		if ok {
			return true
		}

		timeout := time.Duration(500+rand.Intn(1000)) * time.Millisecond
		timer.Reset(timeout)

		select {
		case <-stop:
			timer.Stop()
			return false
		case <-timer.C:
		}
	}
}

func (p *Processor) unlockWorker(id int32) {
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
