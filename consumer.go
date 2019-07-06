package taskq

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"

	"github.com/vmihailenco/taskq/v2/internal"
	"github.com/vmihailenco/taskq/v2/internal/redislock"
)

const stopTimeout = 30 * time.Second

var ErrAsyncTask = errors.New("taskq: async task")

type Delayer interface {
	Delay() time.Duration
}

type ConsumerStats struct {
	WorkerNumber  uint32
	FetcherNumber uint32
	BufferSize    uint32
	Buffered      uint32
	InFlight      uint32
	Processed     uint32
	Retries       uint32
	Fails         uint32
}

//------------------------------------------------------------------------------

// Consumer reserves messages from the queue, processes them,
// and then either releases or deletes messages from the queue.
type Consumer struct {
	q   Queue
	opt *QueueOptions

	buffer  chan *Message // never closed
	limiter *limiter

	startStopMu sync.Mutex
	stopCh      chan struct{}

	fetcherUnsupported int32
	workerNumber       int32 // atomic
	fetcherNumber      int32 // atomic

	fetchersWG sync.WaitGroup
	workersWG  sync.WaitGroup

	errCount uint32
	delaySec uint32

	tunerStats    tunerStats
	tunerRollback func()

	inFlight  uint32
	processed uint32
	fails     uint32
	retries   uint32

	hooks []ConsumerHook
}

// New creates new Consumer for the queue using provided processing options.
func NewConsumer(q Queue) *Consumer {
	opt := q.Options()
	p := &Consumer{
		q:   q,
		opt: opt,

		buffer: make(chan *Message, opt.BufferSize),

		limiter: &limiter{
			bucket:  q.Name(),
			limiter: opt.RateLimiter,
			limit:   opt.RateLimit,
		},
	}
	return p
}

// Starts creates new Consumer and starts it.
func StartConsumer(ctx context.Context, q Queue) *Consumer {
	c := NewConsumer(q)
	if err := c.Start(ctx); err != nil {
		panic(err)
	}
	return c
}

// AddHook adds a hook into message processing.
func (c *Consumer) AddHook(hook ConsumerHook) {
	c.hooks = append(c.hooks, hook)
}

func (c *Consumer) Queue() Queue {
	return c.q
}

func (c *Consumer) Options() *QueueOptions {
	return c.opt
}

func (c *Consumer) Len() int {
	return len(c.buffer)
}

// Stats returns processor stats.
func (c *Consumer) Stats() *ConsumerStats {
	return &ConsumerStats{
		WorkerNumber:  uint32(atomic.LoadInt32(&c.workerNumber)),
		FetcherNumber: uint32(atomic.LoadInt32(&c.fetcherNumber)),
		BufferSize:    uint32(cap(c.buffer)),
		Buffered:      uint32(len(c.buffer)),
		InFlight:      atomic.LoadUint32(&c.inFlight),
		Processed:     atomic.LoadUint32(&c.processed),
		Retries:       atomic.LoadUint32(&c.retries),
		Fails:         atomic.LoadUint32(&c.fails),
	}
}

func (c *Consumer) Add(msg *Message) error {
	if msg.Delay > 0 {
		time.AfterFunc(msg.Delay, func() {
			msg.Delay = 0
			c.add(msg)
		})
	} else {
		c.add(msg)
	}
	return nil
}

func (c *Consumer) add(msg *Message) {
	_ = c.limiter.Reserve(1)
	c.buffer <- msg
}

// Start starts consuming messages in the queue.
func (c *Consumer) Start(ctx context.Context) error {
	c.startStopMu.Lock()
	defer c.startStopMu.Unlock()

	if c.stopCh != nil {
		return errors.New("taksq: Consumer is already started")
	}
	c.stopCh = make(chan struct{})

	for i := 0; i < c.opt.MinWorkers; i++ {
		c.addWorker(ctx)
	}

	c.fetchersWG.Add(1)
	go func() {
		defer c.fetchersWG.Done()
		c.autotune(ctx)
	}()

	return nil
}

// Stop is StopTimeout with 30 seconds timeout.
func (c *Consumer) Stop() error {
	return c.StopTimeout(stopTimeout)
}

// StopTimeout waits workers for timeout duration to finish processing current
// messages and stops workers.
func (c *Consumer) StopTimeout(timeout time.Duration) error {
	c.startStopMu.Lock()
	defer c.startStopMu.Unlock()

	if c.stopCh == nil {
		return errors.New("taksq: Consumer is not started")
	}
	select {
	case <-c.stopCh:
		return errors.New("taksq: Consumer is already stopped")
	default:
	}

	close(c.stopCh)
	defer func() {
		c.stopCh = nil
		atomic.StoreInt32(&c.fetcherNumber, 0)
		atomic.StoreInt32(&c.workerNumber, 0)
	}()

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	done := make(chan struct{}, 1)
	go func() {
		c.fetchersWG.Wait()
		done <- struct{}{}
	}()

	select {
	case <-done:
	case <-timer.C:
		return fmt.Errorf("taskq: %s: fetchers are not stopped after %s", c, timeout)
	}

	go func() {
		c.workersWG.Wait()
		done <- struct{}{}
	}()

	select {
	case <-done:
	case <-timer.C:
		return fmt.Errorf("taskq: %s: workers are not stopped after %s", c, timeout)
	}

	return nil
}

func (c *Consumer) stopped() bool {
	select {
	case <-c.stopCh:
		return true
	default:
		return false
	}
}

func (c *Consumer) paused() time.Duration {
	if c.opt.PauseErrorsThreshold == 0 ||
		atomic.LoadUint32(&c.errCount) < uint32(c.opt.PauseErrorsThreshold) {
		return 0
	}

	sec := atomic.LoadUint32(&c.delaySec)
	if sec == 0 {
		return time.Minute
	}
	return time.Duration(sec) * time.Second
}

func (c *Consumer) addWorker(ctx context.Context) int32 {
	for {
		id := atomic.LoadInt32(&c.workerNumber)
		if id >= int32(c.opt.MaxWorkers) {
			return -1
		}
		if atomic.CompareAndSwapInt32(&c.workerNumber, id, id+1) {
			c.workersWG.Add(1)
			go func() {
				defer c.workersWG.Done()
				c.worker(ctx, id)
			}()
			return id
		}
	}
}

func (c *Consumer) removeWorker(num int32) bool {
	return atomic.CompareAndSwapInt32(&c.workerNumber, num+1, num)
}

func (c *Consumer) addFetcher() int32 {
	if atomic.LoadInt32(&c.fetcherUnsupported) == 1 {
		return -1
	}
	for {
		id := atomic.LoadInt32(&c.fetcherNumber)
		if id >= int32(c.opt.MaxFetchers) {
			return -1
		}
		if c.tryStartFetcher(id) {
			return id
		}
	}
}

func (c *Consumer) tryStartFetcher(id int32) bool {
	if atomic.CompareAndSwapInt32(&c.fetcherNumber, id, id+1) {
		c.fetchersWG.Add(1)
		go func() {
			defer c.fetchersWG.Done()
			c.fetcher(id)
		}()
		return true
	}
	return false
}

func (c *Consumer) removeFetcher(num int32) bool {
	return atomic.CompareAndSwapInt32(&c.fetcherNumber, num+1, num)
}

// ProcessAll starts workers to process messages in the queue and then stops
// them when all messages are processed.
func (c *Consumer) ProcessAll(ctx context.Context) error {
	if err := c.Start(ctx); err != nil {
		return err
	}

	var prev *ConsumerStats
	var noWork int
	for {
		st := c.Stats()
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

	return c.Stop()
}

// ProcessOne processes at most one message in the queue.
func (c *Consumer) ProcessOne(ctx context.Context) error {
	msg, err := c.reserveOne()
	if err != nil {
		return err
	}

	// TODO: wait
	msg.Ctx = ctx
	return c.Process(msg)
}

func (c *Consumer) reserveOne() (*Message, error) {
	select {
	case msg := <-c.buffer:
		return msg, nil
	default:
	}

	msgs, err := c.q.ReserveN(1, c.opt.WaitTimeout)
	if err != nil && err != internal.ErrNotSupported {
		return nil, err
	}

	if len(msgs) == 0 {
		return nil, errors.New("taskq: queue is empty")
	}
	if len(msgs) != 1 {
		return nil, fmt.Errorf("taskq: queue returned %d messages", len(msgs))
	}

	return &msgs[0], nil
}

func (c *Consumer) fetcher(fetcherID int32) {
	timer := time.NewTimer(time.Minute)
	timer.Stop()

	fetchTimeout := c.opt.ReservationTimeout
	fetchTimeout -= fetchTimeout / 10

	for {
		if c.stopped() || fetcherID >= atomic.LoadInt32(&c.fetcherNumber) {
			return
		}

		if pauseTime := c.paused(); pauseTime > 0 {
			c.resetPause()
			internal.Logger.Printf("%s is automatically paused for dur=%s", c, pauseTime)
			time.Sleep(pauseTime)
			continue
		}

		timeout, err := c.fetchMessages(timer, fetchTimeout)
		if err != nil {
			if err == internal.ErrNotSupported {
				atomic.StoreInt32(&c.fetcherUnsupported, 1)
				c.removeFetcher(fetcherID)
				continue
			}

			const backoff = time.Second
			internal.Logger.Printf(
				"%s fetchMessages failed: %s (sleeping for dur=%s)",
				c, err, backoff)
			time.Sleep(backoff)
		}
		if timeout {
			c.removeFetcher(fetcherID)
		}
	}
}

func (c *Consumer) fetchMessages(
	timer *time.Timer, timeout time.Duration,
) (bool, error) {
	size := c.limiter.Reserve(c.opt.ReservationSize)
	msgs, err := c.q.ReserveN(size, c.opt.WaitTimeout)
	if err != nil {
		return false, err
	}

	if d := size - len(msgs); d > 0 {
		c.limiter.Cancel(d)
		c.tunerStats.incFetcherIdle(d)
	} else {
		c.tunerStats.incFetcherBusy()
	}

	timer.Reset(timeout)
	for i := range msgs {
		msg := &msgs[i]

		select {
		case c.buffer <- msg:
		case <-timer.C:
			for i := range msgs[i:] {
				_ = c.q.Release(&msgs[i])
			}
			return true, nil
		}
	}

	if !timer.Stop() {
		<-timer.C
	}

	return false, nil
}

func (c *Consumer) worker(ctx context.Context, workerID int32) {
	var lock *redislock.Lock
	defer func() {
		if lock != nil {
			_ = lock.Release()
		}
	}()

	timer := time.NewTimer(time.Minute)
	timer.Stop()

	for {
		if c.opt.WorkerLimit > 0 {
			lock = c.lockWorker(lock, workerID)
		} else if workerID >= atomic.LoadInt32(&c.workerNumber) {
			return
		}

		msg, timeout := c.waitMessage(timer)
		if timeout {
			continue
		}
		if msg == nil {
			return
		}

		msg.Ctx = ctx
		_ = c.Process(msg)
	}
}

func (c *Consumer) waitMessage(timer *time.Timer) (_ *Message, timeout bool) {
	const workerIdleTimeout = time.Second

	select {
	case msg := <-c.buffer:
		c.tunerStats.incWorkerBusy()
		return msg, false
	default:
	}

	c.tunerStats.incWorkerIdle(1)

	if atomic.LoadInt32(&c.fetcherUnsupported) == 0 {
		c.tryStartFetcher(0)
	}

	timer.Reset(workerIdleTimeout)
	select {
	case msg := <-c.buffer:
		if !timer.Stop() {
			<-timer.C
		}
		return msg, false
	case <-c.stopCh:
		return nil, false
	case <-timer.C:
		c.tunerStats.incWorkerIdle(2)
		return nil, true
	}
}

// Process is low-level API to process message bypassing the internal queue.
func (c *Consumer) Process(msg *Message) error {
	atomic.AddUint32(&c.inFlight, 1)

	if msg.Delay > 0 {
		err := c.q.Add(msg)
		if err != nil {
			return err
		}
		c.delete(msg)
		return nil
	}

	if msg.StickyErr != nil {
		c.Put(msg)
		return msg.StickyErr
	}

	evt, err := c.beforeProcessMessage(msg)
	if err != nil {
		return c.handleError(msg, err)
	}

	msgErr := c.handleMessage(msg)

	err = c.afterProcessMessage(evt, msgErr)
	if err != nil {
		return c.handleError(msg, err)
	}

	return c.handleError(msg, msgErr)
}

func (c *Consumer) handleMessage(msg *Message) error {
	return Tasks.HandleMessage(msg)
}

func (c *Consumer) handleError(msg *Message, err error) error {
	if err == nil {
		c.resetPause()
	}
	if err != ErrAsyncTask {
		msg.StickyErr = err
		c.Put(msg)
	}
	return err
}

func (c *Consumer) Put(msg *Message) {
	if msg.StickyErr == nil {
		atomic.AddUint32(&c.processed, 1)
		c.tunerStats.incProcessed()
		c.delete(msg)
		return
	}

	task := Tasks.Get(msg.TaskName)
	var opt *TaskOptions
	if task != nil {
		opt = task.Options()
	} else {
		opt = unknownTaskOpt
	}

	atomic.AddUint32(&c.errCount, 1)
	if msg.ReservedCount < opt.RetryLimit {
		msg.Delay = exponentialBackoff(
			opt.MinBackoff, opt.MaxBackoff, msg.ReservedCount)
		if msg.StickyErr != nil {
			if delayer, ok := msg.StickyErr.(Delayer); ok {
				msg.Delay = delayer.Delay()
			}
		}

		atomic.AddUint32(&c.retries, 1)
		c.release(msg)
	} else {
		atomic.AddUint32(&c.fails, 1)
		c.delete(msg)
	}
}

func (c *Consumer) release(msg *Message) {
	if msg.StickyErr != nil {
		new := uint32(msg.Delay / time.Second)
		for new > 0 {
			old := atomic.LoadUint32(&c.delaySec)
			if new > old {
				break
			}
			if atomic.CompareAndSwapUint32(&c.delaySec, old, new) {
				break
			}
		}

		internal.Logger.Printf("task=%q failed (will retry=%d in dur=%s): %s",
			msg.TaskName, msg.ReservedCount, msg.Delay, msg.StickyErr)
	}

	msg.StickyErr = nil
	err := c.q.Release(msg)
	if err != nil {
		internal.Logger.Printf("task=%q Release failed: %s", msg.TaskName, err)
	}
	atomic.AddUint32(&c.inFlight, ^uint32(0))
}

func (c *Consumer) delete(msg *Message) {
	if msg.StickyErr != nil {
		internal.Logger.Printf("task=%q handler failed after retry=%d: %s",
			msg.TaskName, msg.ReservedCount, msg.StickyErr)

		err := c.handleMessage(msg)
		if err != nil {
			internal.Logger.Printf("task=%q fallback handler failed: %s", msg.TaskName, err)
		}
	}

	msg.StickyErr = nil
	err := c.q.Delete(msg)
	if err != nil {
		internal.Logger.Printf("taks=%q Delete failed: %s", msg.TaskName, err)
	}
	atomic.AddUint32(&c.inFlight, ^uint32(0))
}

// Purge discards messages from the internal queue.
func (c *Consumer) Purge() error {
	for {
		select {
		case msg := <-c.buffer:
			c.delete(msg)
		default:
			return nil
		}
	}
}

type ProcessMessageEvent struct {
	Message   *Message
	StartTime time.Time
	Error     error

	Stash map[interface{}]interface{}
}

type ConsumerHook interface {
	BeforeProcessMessage(*ProcessMessageEvent) error
	AfterProcessMessage(*ProcessMessageEvent) error
}

func (c *Consumer) beforeProcessMessage(msg *Message) (*ProcessMessageEvent, error) {
	if len(c.hooks) == 0 {
		return nil, nil
	}
	evt := &ProcessMessageEvent{
		Message:   msg,
		StartTime: time.Now(),
	}
	for _, hook := range c.hooks {
		err := hook.BeforeProcessMessage(evt)
		if err != nil {
			return nil, err
		}
	}
	return evt, nil
}

func (c *Consumer) afterProcessMessage(evt *ProcessMessageEvent, err error) error {
	if evt == nil {
		return nil
	}
	evt.Error = err
	for _, hook := range c.hooks {
		err := hook.AfterProcessMessage(evt)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *Consumer) resetPause() {
	atomic.StoreUint32(&c.delaySec, 0)
	atomic.StoreUint32(&c.errCount, 0)
}

func (c *Consumer) lockWorker(lock *redislock.Lock, workerID int32) *redislock.Lock {
	timeout := c.opt.ReservationTimeout + 10*time.Second

	timer := time.NewTimer(time.Minute)
	timer.Stop()

	for {
		var err error
		if lock == nil {
			key := fmt.Sprintf("%s:worker:lock:%d", c.q.Name(), workerID)
			lock, err = redislock.Obtain(c.opt.Redis, key, timeout, nil)
		} else {
			err = lock.Refresh(timeout, nil)
		}
		if err == nil {
			return lock
		}

		if err != redislock.ErrNotObtained {
			internal.Logger.Printf("redislock.Lock failed: %s", err)
		}
		lock = nil

		timeout := time.Duration(500+rand.Intn(500)) * time.Millisecond
		timer.Reset(timeout)

		select {
		case <-c.stopCh:
			if !timer.Stop() {
				<-timer.C
			}
			return lock
		case <-timer.C:
		}
	}
}

func (c *Consumer) String() string {
	fnum := atomic.LoadInt32(&c.fetcherNumber)
	wnum := atomic.LoadInt32(&c.workerNumber)
	inFlight := atomic.LoadUint32(&c.inFlight)
	processed := atomic.LoadUint32(&c.processed)
	fails := atomic.LoadUint32(&c.fails)

	return fmt.Sprintf(
		"Consumer<%s %d/%d/%d %d/%d %d/%d>",
		c.q.Name(),
		fnum, len(c.buffer), cap(c.buffer),
		inFlight, wnum,
		processed, fails)
}

func (c *Consumer) autotune(ctx context.Context) {
	timer := time.NewTicker(100 * time.Millisecond)
	for {
		select {
		case <-timer.C:
			c.tunerTick(ctx)
		case <-c.stopCh:
			return
		}
	}
}

func (c *Consumer) tunerTick(ctx context.Context) {
	if c.tunerStats.ticks >= 10 {
		c.tune(ctx)
	}

	buffered := len(c.buffer)
	if buffered < cap(c.buffer)/5 {
		c.tunerStats.starving++
	} else if buffered > cap(c.buffer)*4/5 {
		c.tunerStats.loaded++
	}
	c.tunerStats.ticks++
}

func (c *Consumer) tune(ctx context.Context) {
	if c.tunerRollback != nil {
		rollback := c.tunerRollback
		c.tunerRollback = nil

		rate := c.tunerStats.rate()
		prevRate := c.tunerStats.prevRate
		if rate < prevRate {
			rollback()
			c.tunerStats.reset()
			c.tunerStats.prevRate = prevRate
			return
		}
	}

	if c.tunerStats.isStarving() {
		if c.tunerAddFetcher() {
			return
		}
	}

	if c.opt.WorkerLimit == 0 && c.tunerStats.isLoaded() {
		if id := c.addWorker(ctx); id != -1 {
			internal.Logger.Printf("%s: added worker=%d", c, id)
			c.tunerRollback = func() {
				if c.removeWorker(id) {
					internal.Logger.Printf("%s: remove added worker=%d", c, id)
				}
			}
			c.tunerStats.reset()
		}
		return
	}

	var reset bool

	if id := c.idleFetcher(); id != -1 {
		if c.removeFetcher(id) {
			internal.Logger.Printf("%s: removed idle fetcher=%d", c, id)
		}
		reset = true
	}

	if c.opt.WorkerLimit == 0 {
		if id := c.idleWorker(); id != -1 {
			if c.removeWorker(id) {
				internal.Logger.Printf("%s: removed idle worker=%d", c, id)
			}
			reset = true
		}
	}

	if reset || c.tunerStats.ticks >= 100 {
		c.tunerStats.reset()
	}
}

func (c *Consumer) tunerAddFetcher() bool {
	id := c.addFetcher()
	if id == -1 {
		return false
	}
	internal.Logger.Printf("%s: added fetcher=%d", c, id)
	c.tunerRollback = func() {
		if c.removeFetcher(id) {
			internal.Logger.Printf("%s: remove added fetcher=%d", c, id)
		}
	}
	c.tunerStats.reset()
	return true
}

func (c *Consumer) idleFetcher() int32 {
	num := atomic.LoadInt32(&c.fetcherNumber)
	if num == 0 || (num == 1 && !c.tunerStats.workersStuck()) {
		return -1
	}
	if c.tunerStats.hasIdleFetcher(num) {
		return num - 1
	}
	return -1
}

func (c *Consumer) idleWorker() int32 {
	num := atomic.LoadInt32(&c.workerNumber)
	if num <= 1 {
		return -1
	}
	if c.tunerStats.hasIdleWorker(num) {
		return num - 1
	}
	return -1
}

//------------------------------------------------------------------------------

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

//------------------------------------------------------------------------------

type tunerStats struct {
	processed uint32 // atomic
	prevRate  float64

	ticks    int
	starving int
	loaded   int

	fetcherIdle uint32 // atomic
	fetcherBusy uint32 // atomic

	workerIdle uint32 // atomic
	workerBusy uint32 // atomic
}

func (s *tunerStats) reset() {
	s.prevRate = s.rate()
	atomic.StoreUint32(&s.processed, 0)

	s.ticks = 0
	s.starving = 0
	s.loaded = 0

	atomic.StoreUint32(&s.fetcherIdle, 0)
	atomic.StoreUint32(&s.fetcherBusy, 0)

	atomic.StoreUint32(&s.workerIdle, 0)
	atomic.StoreUint32(&s.workerBusy, 0)
}

func (s *tunerStats) incProcessed() {
	atomic.AddUint32(&s.processed, 1)
}

func (s *tunerStats) rate() float64 {
	n := atomic.LoadUint32(&s.processed)
	return float64(n) / float64(s.ticks)
}

func (s *tunerStats) getFetcherIdle() uint32 {
	return atomic.LoadUint32(&s.fetcherIdle)
}

func (s *tunerStats) incFetcherIdle(n int) {
	atomic.AddUint32(&s.fetcherIdle, uint32(n))
}

func (s *tunerStats) getFetcherBusy() uint32 {
	return atomic.LoadUint32(&s.fetcherBusy)
}

func (s *tunerStats) incFetcherBusy() {
	atomic.AddUint32(&s.fetcherBusy, 1)
}

func (s *tunerStats) hasIdleFetcher(num int32) bool {
	idle := s.getFetcherIdle()
	busy := s.getFetcherBusy()
	return hasIdleUnit(idle, busy, num)
}

func (s *tunerStats) getWorkerIdle() uint32 {
	return atomic.LoadUint32(&s.workerIdle)
}

func (s *tunerStats) incWorkerIdle(n int) {
	atomic.AddUint32(&s.workerIdle, uint32(n))
}

func (s *tunerStats) getWorkerBusy() uint32 {
	return atomic.LoadUint32(&s.workerBusy)
}

func (s *tunerStats) incWorkerBusy() {
	atomic.AddUint32(&s.workerBusy, 1)
}

func (s *tunerStats) workersStuck() bool {
	idle := s.getWorkerIdle()
	busy := s.getWorkerBusy()
	return idle+busy == 0
}

func (s *tunerStats) hasIdleWorker(num int32) bool {
	idle := s.getWorkerIdle()
	busy := s.getWorkerBusy()
	return hasIdleUnit(idle, busy, num)
}

func hasIdleUnit(idle, busy uint32, num int32) bool {
	return idle+busy >= 10 && float64(idle) > 2*(float64(busy)/float64(num))
}

func (s *tunerStats) isStarving() bool {
	if s.starving+s.loaded < 5 || float32(s.starving)/float32(s.loaded) < 2 {
		return false
	}
	idle := s.getFetcherIdle()
	busy := s.getFetcherBusy()
	return isBusy(idle, busy)
}

func (s *tunerStats) isLoaded() bool {
	if s.starving+s.loaded < 5 || float32(s.loaded)/float32(s.starving) < 2 {
		return false
	}
	idle := s.getWorkerIdle()
	busy := s.getWorkerBusy()
	return isBusy(idle, busy)
}

func isBusy(idle, busy uint32) bool {
	return idle+busy >= 10 && busy > idle
}

//------------------------------------------------------------------------------

func exponentialBackoff(min, max time.Duration, retry int) time.Duration {
	var d time.Duration
	if retry > 0 {
		d = min << uint(retry-1)
	}
	if d < min {
		return min
	}
	if d > max {
		return max
	}
	return d
}
