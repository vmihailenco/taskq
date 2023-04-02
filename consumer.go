package taskq

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-redis/redis_rate/v10"

	"github.com/vmihailenco/taskq/v3/internal"
)

const stopTimeout = 30 * time.Second

var ErrAsyncTask = errors.New("taskq: async task")

type Delayer interface {
	Delay() time.Duration
}

type ConsumerStats struct {
	NumWorker  uint32
	NumFetcher uint32

	BufferSize uint32
	Buffered   uint32

	InFlight  uint32
	Processed uint32
	Retries   uint32
	Fails     uint32
}

//------------------------------------------------------------------------------

const (
	stateInit = iota
	stateStarted
	stateStoppingFetchers
	stateStoppingWorkers
)

// Consumer reserves messages from the queue, processes them,
// and then either releases or deletes messages from the queue.
type Consumer struct {
	q   Queue
	opt *QueueOptions

	buffer  chan *Message // never closed
	limiter *limiter

	consecutiveNumErr uint32

	inFlight  uint32
	processed uint32
	fails     uint32
	retries   uint32

	hooks []ConsumerHook

	startStopMu sync.Mutex

	fetchersCtx  context.Context
	fetchersWG   sync.WaitGroup
	stopFetchers func()

	workersCtx  context.Context
	workersWG   sync.WaitGroup
	stopWorkers func()
}

// NewConsumer creates new Consumer for the queue using provided processing options.
func NewConsumer(q Queue) *Consumer {
	opt := q.Options()
	c := &Consumer{
		q:   q,
		opt: opt,

		buffer: make(chan *Message, opt.BufferSize),

		limiter: &limiter{
			bucket:  q.Name(),
			limiter: opt.RateLimiter,
			limit:   opt.RateLimit,
		},
	}
	return c
}

// StartConsumer creates new QueueConsumer and starts it.
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
		BufferSize: uint32(cap(c.buffer)),
		Buffered:   uint32(len(c.buffer)),

		InFlight:  atomic.LoadUint32(&c.inFlight),
		Processed: atomic.LoadUint32(&c.processed),
		Retries:   atomic.LoadUint32(&c.retries),
		Fails:     atomic.LoadUint32(&c.fails),
	}
}

func (c *Consumer) Add(msg *Message) error {
	_, _ = c.limiter.Reserve(msg.Ctx, 1)
	c.buffer <- msg
	return nil
}

// Start starts consuming messages in the queue.
func (c *Consumer) Start(ctx context.Context) error {
	c.startStopMu.Lock()
	defer c.startStopMu.Unlock()

	if c.fetchersCtx != nil || c.workersCtx != nil {
		return nil
	}

	c.workersCtx, c.stopWorkers = context.WithCancel(ctx)
	for i := 0; i < c.opt.NumWorker; i++ {
		i := i
		c.workersWG.Add(1)
		go func() {
			defer c.workersWG.Done()
			c.worker(c.workersCtx, i)
		}()
	}

	c.fetchersCtx, c.stopFetchers = context.WithCancel(ctx)
	for i := 0; i < c.opt.NumFetcher; i++ {
		i := i
		c.fetchersWG.Add(1)
		go func() {
			defer c.fetchersWG.Done()
			c.fetcher(c.fetchersCtx, i)
		}()
	}

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

	if c.fetchersCtx == nil || c.workersCtx == nil {
		return nil
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()
	done := make(chan struct{}, 1)

	go func() {
		c.fetchersWG.Wait()
		done <- struct{}{}
	}()

	var firstErr error

	c.stopFetchers()
	select {
	case <-done:
	case <-timer.C:
		if firstErr == nil {
			firstErr = fmt.Errorf("taskq: %s: fetchers are not stopped after %s", c, timeout)
		}
	}

	go func() {
		c.workersWG.Wait()
		done <- struct{}{}
	}()

	c.stopWorkers()
	select {
	case <-done:
	case <-timer.C:
		if firstErr == nil {
			firstErr = fmt.Errorf("taskq: %s: workers are not stopped after %s", c, timeout)
		}
	}

	c.fetchersCtx = nil
	c.workersCtx = nil

	return firstErr
}

func (c *Consumer) paused() time.Duration {
	if c.opt.PauseErrorsThreshold == 0 ||
		atomic.LoadUint32(&c.consecutiveNumErr) < uint32(c.opt.PauseErrorsThreshold) {
		return 0
	}
	return time.Minute
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
	msg, err := c.reserveOne(ctx)
	if err != nil {
		return err
	}

	msg.Ctx = ctx
	return c.Process(ctx, msg)
}

func (c *Consumer) reserveOne(ctx context.Context) (*Message, error) {
	select {
	case msg := <-c.buffer:
		return msg, nil
	default:
	}

	msgs, err := c.q.ReserveN(ctx, 1, c.opt.WaitTimeout)
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

func (c *Consumer) fetcher(ctx context.Context, fetcherID int) {
	timer := time.NewTimer(time.Minute)
	timer.Stop()

	for {
		if pauseTime := c.paused(); pauseTime > 0 {
			internal.Logger.Printf("%s is automatically paused for dur=%s", c, pauseTime)
			time.Sleep(pauseTime)
			c.resetPause()
			continue
		}

		switch err := c.fetchMessages(ctx); err {
		case nil:
			// nothing
		case internal.ErrNotSupported, context.Canceled:
			return
		default:
			backoff := time.Second
			internal.Logger.Printf(
				"%s fetchMessages failed: %s (sleeping for dur=%s)",
				c, err, backoff)
			sleep(ctx, backoff)
		}
	}
}

func (c *Consumer) fetchMessages(ctx context.Context) error {
	size, err := c.limiter.Reserve(ctx, c.opt.ReservationSize)
	if err != nil {
		return err
	}

	msgs, err := c.q.ReserveN(ctx, size, c.opt.WaitTimeout)
	if err != nil {
		return err
	}

	if d := size - len(msgs); d > 0 {
		c.limiter.Cancel(d)
	}

	for i := range msgs {
		msg := &msgs[i]
		select {
		case c.buffer <- msg:
		case <-ctx.Done():
			for i := range msgs[i:] {
				_ = c.q.Release(ctx, &msgs[i])
			}
			return context.Canceled
		}
	}

	return nil
}

func (c *Consumer) worker(ctx context.Context, workerID int) {
	for {
		msg := c.waitMessage(ctx)
		if msg == nil {
			return
		}
		msg.Ctx = ctx
		_ = c.Process(internal.UndoContext(ctx), msg)
	}
}

func (c *Consumer) waitMessage(ctx context.Context) *Message {
	select {
	case msg := <-c.buffer:
		return msg
	default:
	}

	select {
	case msg := <-c.buffer:
		return msg
	case <-ctx.Done():
		return nil
	}
}

// Process is low-level API to process message bypassing the internal queue.
func (c *Consumer) Process(ctx context.Context, msg *Message) error {
	atomic.AddUint32(&c.inFlight, 1)

	if msg.Delay > 0 {
		if err := c.q.Add(ctx, msg); err != nil {
			return err
		}
		return nil
	}

	if msg.Err != nil {
		msg.Delay = -1
		c.Put(ctx, msg)
		return msg.Err
	}

	evt, err := c.beforeProcessMessage(msg)
	if err != nil {
		msg.Err = err
		c.Put(ctx, msg)
		return err
	}

	msg.evt = evt

	msgErr := c.opt.Handler.HandleMessage(msg)
	if msgErr == ErrAsyncTask {
		return ErrAsyncTask
	}

	msg.Err = msgErr
	c.Put(ctx, msg)

	return msg.Err
}

func (c *Consumer) Put(ctx context.Context, msg *Message) {
	if err := c.afterProcessMessage(msg); err != nil {
		msg.Err = err
	}

	if msg.Err == nil {
		c.resetPause()
		atomic.AddUint32(&c.processed, 1)
		c.delete(ctx, msg)
		return
	}

	atomic.AddUint32(&c.consecutiveNumErr, 1)
	if msg.Delay <= 0 {
		atomic.AddUint32(&c.fails, 1)
		c.delete(ctx, msg)
		return
	}

	atomic.AddUint32(&c.retries, 1)
	c.release(ctx, msg)
}

func (c *Consumer) release(ctx context.Context, msg *Message) {
	if msg.Err != nil {
		internal.Logger.Printf("task=%q failed (will retry=%d in dur=%s): %s",
			msg.TaskName, msg.ReservedCount, msg.Delay, msg.Err)
	}

	if err := c.q.Release(ctx, msg); err != nil {
		internal.Logger.Printf("task=%q Release failed: %s", msg.TaskName, err)
	}
	atomic.AddUint32(&c.inFlight, ^uint32(0))
}

func (c *Consumer) delete(ctx context.Context, msg *Message) {
	if msg.Err != nil {
		internal.Logger.Printf("task=%q handler failed after retry=%d: %s",
			msg.TaskName, msg.ReservedCount, msg.Err)

		if err := c.opt.Handler.HandleMessage(msg); err != nil {
			internal.Logger.Printf("task=%q fallback handler failed: %s", msg.TaskName, err)
		}
	}

	if err := c.q.Delete(ctx, msg); err != nil {
		internal.Logger.Printf("task=%q Delete failed: %s", msg.TaskName, err)
	}
	atomic.AddUint32(&c.inFlight, ^uint32(0))
}

// Purge discards messages from the internal queue.
func (c *Consumer) Purge(ctx context.Context) error {
	for {
		select {
		case msg := <-c.buffer:
			c.delete(ctx, msg)
		default:
			return nil
		}
	}
}

type ProcessMessageEvent struct {
	Message   *Message
	StartTime time.Time

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
		if err := hook.BeforeProcessMessage(evt); err != nil {
			return nil, err
		}
	}

	return evt, nil
}

func (c *Consumer) afterProcessMessage(msg *Message) error {
	if msg.evt == nil {
		return nil
	}

	var firstErr error
	for _, hook := range c.hooks {
		if err := hook.AfterProcessMessage(msg.evt); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (c *Consumer) resetPause() {
	atomic.StoreUint32(&c.consecutiveNumErr, 0)
}

func (c *Consumer) String() string {
	inFlight := atomic.LoadUint32(&c.inFlight)
	processed := atomic.LoadUint32(&c.processed)
	retries := atomic.LoadUint32(&c.retries)
	fails := atomic.LoadUint32(&c.fails)

	return fmt.Sprintf(
		"%s %d/%d/%d %d/%d/%d",
		c.q.Name(),
		inFlight, len(c.buffer), cap(c.buffer),
		processed, retries, fails)
}

//------------------------------------------------------------------------------

type limiter struct {
	bucket  string
	limiter *redis_rate.Limiter
	limit   redis_rate.Limit

	allowedCount uint32 // atomic
	cancelled    uint32 // atomic
}

func (l *limiter) Reserve(ctx context.Context, max int) (int, error) {
	if l.limiter == nil || l.limit.IsZero() {
		return max, nil
	}

	for {
		cancelled := atomic.LoadUint32(&l.cancelled)
		if cancelled == 0 {
			break
		}

		if cancelled >= uint32(max) {
			if atomic.CompareAndSwapUint32(&l.cancelled, cancelled, uint32(max)-1) {
				return max, nil
			}
			continue
		}

		if atomic.CompareAndSwapUint32(&l.cancelled, cancelled, uint32(cancelled)-1) {
			return int(cancelled), nil
		}
	}

	for {
		res, err := l.limiter.AllowAtMost(ctx, l.bucket, l.limit, max)
		if err != nil {
			if err == context.Canceled {
				return 0, err
			}
			time.Sleep(100 * time.Millisecond)
			continue
		}

		if res.Allowed > 0 {
			atomic.AddUint32(&l.allowedCount, 1)
			return res.Allowed, nil
		}

		atomic.StoreUint32(&l.allowedCount, 0)
		sleep(ctx, res.RetryAfter)
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

func sleep(ctx context.Context, d time.Duration) error {
	done := ctx.Done()
	if done == nil {
		time.Sleep(d)
		return nil
	}

	t := time.NewTimer(d)
	defer t.Stop()

	select {
	case <-t.C:
		return nil
	case <-done:
		return ctx.Err()
	}
}
