package taskq

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-redis/redis_rate/v9"

	"github.com/vmihailenco/taskq/v3/internal"
	"github.com/vmihailenco/taskq/v3/internal/redislock"
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
	Timing    time.Duration
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

	startStopMu sync.Mutex
	state       int32 // atomic
	stopCh      chan struct{}

	cfgs       *configRoulette
	numWorker  int32 // atomic
	numFetcher int32 // atomic

	fetchersWG sync.WaitGroup
	workersWG  sync.WaitGroup

	consecutiveNumErr uint32
	queueEmptyVote    int32

	inFlight  uint32
	processed uint32
	fails     uint32
	retries   uint32
	timings   sync.Map

	hooks []ConsumerHook
}

// New creates new Consumer for the queue using provided processing options.
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
		NumWorker:  uint32(atomic.LoadInt32(&c.numWorker)),
		NumFetcher: uint32(atomic.LoadInt32(&c.numFetcher)),

		BufferSize: uint32(cap(c.buffer)),
		Buffered:   uint32(len(c.buffer)),

		InFlight:  atomic.LoadUint32(&c.inFlight),
		Processed: atomic.LoadUint32(&c.processed),
		Retries:   atomic.LoadUint32(&c.retries),
		Fails:     atomic.LoadUint32(&c.fails),

		Timing: c.timing(),
	}
}

func (c *Consumer) Add(msg *Message) error {
	_ = c.limiter.Reserve(msg.Ctx, 1)
	c.buffer <- msg
	return nil
}

// Start starts consuming messages in the queue.
func (c *Consumer) Start(ctx context.Context) error {
	if err := c.start(); err != nil {
		return err
	}

	if c.opt.MinNumWorker < c.opt.MaxNumWorker {
		c.cfgs = newConfigRoulette(c.opt)
		cfg := c.cfgs.Select(nil, false)
		c.replaceConfig(ctx, cfg)

		c.fetchersWG.Add(1)
		go func() {
			defer c.fetchersWG.Done()
			c.autotune(ctx, cfg)
		}()
	} else {
		c.replaceConfig(ctx, &consumerConfig{
			NumFetcher: 0, // fetcher is automatically started when needed
			NumWorker:  c.opt.MinNumWorker,
		})
	}

	return nil
}

func (c *Consumer) start() error {
	c.startStopMu.Lock()
	defer c.startStopMu.Unlock()

	switch atomic.LoadInt32(&c.state) {
	case stateInit:
		atomic.StoreInt32(&c.state, stateStarted)
		c.stopCh = make(chan struct{})
	case stateStarted:
		return fmt.Errorf("taskq: Consumer is already started")
	case stateStoppingFetchers, stateStoppingWorkers:
		return fmt.Errorf("taskq: Consumer is stopping")
	}

	atomic.StoreInt32(&c.numFetcher, 0)
	atomic.StoreInt32(&c.numWorker, 0)

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

	switch atomic.LoadInt32(&c.state) {
	case stateInit:
		return fmt.Errorf("taskq: Consumer is not started")
	case stateStarted:
		atomic.StoreInt32(&c.state, stateStoppingFetchers)
		close(c.stopCh)
	case stateStoppingFetchers, stateStoppingWorkers:
		return fmt.Errorf("taskq: Consumer is stopping")
	}

	// Stop all fetchers.
	atomic.StoreInt32(&c.numFetcher, -1)
	defer func() {
		atomic.StoreInt32(&c.numWorker, -1)
		atomic.StoreInt32(&c.state, stateInit)
	}()

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	done := make(chan struct{}, 1)
	go func() {
		c.fetchersWG.Wait()
		done <- struct{}{}
	}()

	var firstErr error
	select {
	case <-done:
	case <-timer.C:
		firstErr = fmt.Errorf("taskq: %s: fetchers are not stopped after %s", c, timeout)
	}

	if !atomic.CompareAndSwapInt32(&c.state, stateStoppingFetchers, stateStoppingWorkers) {
		panic("not reached")
	}
	if firstErr != nil {
		return firstErr
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

func (c *Consumer) paused() time.Duration {
	if c.opt.PauseErrorsThreshold == 0 ||
		atomic.LoadUint32(&c.consecutiveNumErr) < uint32(c.opt.PauseErrorsThreshold) {
		return 0
	}
	return time.Minute
}

func (c *Consumer) addWorker(ctx context.Context, id int32) bool {
	c.startStopMu.Lock()
	defer c.startStopMu.Unlock()

	if atomic.CompareAndSwapInt32(&c.numWorker, id, id+1) {
		c.workersWG.Add(1)
		go func() {
			defer c.workersWG.Done()
			c.worker(ctx, id)
		}()
		return true
	}
	return false
}

func (c *Consumer) removeWorker(id int32) bool { //nolint:unused
	return atomic.CompareAndSwapInt32(&c.numWorker, id+1, id)
}

func (c *Consumer) addFetcher(ctx context.Context, id int32) bool {
	c.startStopMu.Lock()
	defer c.startStopMu.Unlock()

	if atomic.CompareAndSwapInt32(&c.numFetcher, id, id+1) {
		c.fetchersWG.Add(1)
		go func() {
			defer c.fetchersWG.Done()
			c.fetcher(ctx, id)
		}()
		return true
	}
	return false
}

func (c *Consumer) ensureFetcher(ctx context.Context) {
	if atomic.LoadInt32(&c.numFetcher) == 0 {
		c.addFetcher(ctx, 0)
	}
}

func (c *Consumer) removeFetcher(num int32) bool {
	return atomic.CompareAndSwapInt32(&c.numFetcher, num+1, num)
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

	// TODO: wait
	msg.Ctx = ctx
	return c.Process(msg)
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

func (c *Consumer) fetcher(ctx context.Context, fetcherID int32) {
	timer := time.NewTimer(time.Minute)
	timer.Stop()

	fetchTimeout := c.opt.ReservationTimeout
	fetchTimeout -= fetchTimeout / 10

	for {
		if fetcherID >= atomic.LoadInt32(&c.numFetcher) {
			return
		}

		if pauseTime := c.paused(); pauseTime > 0 {
			internal.Logger.Printf("%s is automatically paused for dur=%s", c, pauseTime)
			time.Sleep(pauseTime)
			c.resetPause()
			continue
		}

		timeout, err := c.fetchMessages(ctx, timer, fetchTimeout)
		if err != nil {
			if err == internal.ErrNotSupported {
				atomic.StoreInt32(&c.numFetcher, -1)
				continue
			}

			const backoff = time.Second
			internal.Logger.Printf(
				"%s fetchMessages failed: %s (sleeping for dur=%s)",
				c, err, backoff)
			time.Sleep(backoff)
			continue
		}
		if timeout {
			c.removeFetcher(fetcherID)
		}
	}
}

func (c *Consumer) fetchMessages(
	ctx context.Context, timer *time.Timer, timeout time.Duration,
) (bool, error) {
	size := c.limiter.Reserve(ctx, c.opt.ReservationSize)

	msgs, err := c.q.ReserveN(ctx, size, c.opt.WaitTimeout)
	if err != nil {
		return false, err
	}

	if d := size - len(msgs); d > 0 {
		c.limiter.Cancel(d)
		c.voteQueueEmpty()
	} else {
		c.voteQueueFull()
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
			_ = lock.Release(ctx)
		}
	}()

	timer := time.NewTimer(time.Minute)
	timer.Stop()

	for {
		if workerID >= atomic.LoadInt32(&c.numWorker) {
			return
		}
		if c.opt.WorkerLimit > 0 {
			lock = c.lockWorker(ctx, lock, workerID)
		}

		msg := c.waitMessage(ctx, timer)
		if msg == nil {
			if atomic.LoadInt32(&c.state) >= stateStoppingWorkers {
				return
			}
			continue
		}

		msg.Ctx = ctx
		_ = c.Process(msg)
	}
}

func (c *Consumer) waitMessage(ctx context.Context, timer *time.Timer) *Message {
	const workerIdleTimeout = time.Second

	select {
	case msg := <-c.buffer:
		return msg
	default:
	}

	c.ensureFetcher(ctx)

	timer.Reset(workerIdleTimeout)
	select {
	case msg := <-c.buffer:
		if !timer.Stop() {
			<-timer.C
		}
		return msg
	case <-timer.C:
		c.voteQueueEmpty()
		return nil
	case <-c.stopCh:
		return nil
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

	if msg.Err != nil {
		c.Put(msg)
		return msg.Err
	}

	evt, err := c.beforeProcessMessage(msg)
	if err != nil {
		msg.Err = err
		c.Put(msg)
		return err
	}
	msg.evt = evt

	start := time.Now()
	msgErr := c.opt.Handler.HandleMessage(msg)
	if msgErr == ErrAsyncTask {
		return ErrAsyncTask
	}
	c.updateTiming(msg.TaskName, time.Since(start))

	msg.Err = msgErr
	c.Put(msg)
	return msg.Err
}

func (c *Consumer) updateTiming(taskName string, x time.Duration) {
	const decay = float64(1) / 10

	timing := new(int64)

	if v, loaded := c.timings.LoadOrStore(taskName, timing); loaded {
		timing = v.(*int64)
	}

	for i := 0; i < 100; i++ {
		oldVal := atomic.LoadInt64(timing)

		var newVal int64
		if oldVal != 0 {
			newVal = int64(float64(oldVal)*(1-decay) + float64(x)*decay)
		} else {
			newVal = int64(x)
		}

		if atomic.CompareAndSwapInt64(timing, oldVal, newVal) {
			break
		}
	}
}

func (c *Consumer) timing() time.Duration {
	var mean int64
	c.timings.Range(func(_, value interface{}) bool {
		x := atomic.LoadInt64(value.(*int64))
		if mean != 0 {
			mean = (mean + x) / 2
		} else {
			mean = x
		}
		return true
	})
	return time.Duration(mean)
}

func (c *Consumer) Put(msg *Message) {
	err := c.afterProcessMessage(msg)
	if err != nil {
		msg.Err = err
		return
	}

	if msg.Err == nil {
		c.resetPause()
		atomic.AddUint32(&c.processed, 1)
		c.delete(msg)
		return
	}

	atomic.AddUint32(&c.consecutiveNumErr, 1)
	if msg.Delay == -1 {
		atomic.AddUint32(&c.fails, 1)
		c.delete(msg)
		return
	}

	atomic.AddUint32(&c.retries, 1)
	c.release(msg)
}

func (c *Consumer) release(msg *Message) {
	if msg.Err != nil {
		internal.Logger.Printf("task=%q failed (will retry=%d in dur=%s): %s",
			msg.TaskName, msg.ReservedCount, msg.Delay, msg.Err)
	}

	err := c.q.Release(msg)
	if err != nil {
		internal.Logger.Printf("task=%q Release failed: %s", msg.TaskName, err)
	}
	atomic.AddUint32(&c.inFlight, ^uint32(0))
}

func (c *Consumer) delete(msg *Message) {
	if msg.Err != nil {
		internal.Logger.Printf("task=%q handler failed after retry=%d: %s",
			msg.TaskName, msg.ReservedCount, msg.Err)

		err := c.opt.Handler.HandleMessage(msg)
		if err != nil {
			internal.Logger.Printf("task=%q fallback handler failed: %s", msg.TaskName, err)
		}
	}

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

func (c *Consumer) afterProcessMessage(msg *Message) error {
	if msg.evt == nil {
		return nil
	}
	for _, hook := range c.hooks {
		err := hook.AfterProcessMessage(msg.evt)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *Consumer) resetPause() {
	atomic.StoreUint32(&c.consecutiveNumErr, 0)
}

func (c *Consumer) lockWorker(
	ctx context.Context,
	lock *redislock.Lock,
	workerID int32,
) *redislock.Lock {
	lockTimeout := c.opt.ReservationTimeout + 10*time.Second

	timer := time.NewTimer(time.Minute)
	timer.Stop()

	for {
		var err error
		if lock == nil {
			key := fmt.Sprintf("%s:worker:lock:%d", c.q.Name(), workerID)
			lock, err = redislock.Obtain(ctx, c.opt.Redis, key, lockTimeout, nil)
		} else {
			err = lock.Refresh(ctx, lockTimeout, nil)
		}
		if err == nil {
			return lock
		}

		if err != redislock.ErrNotObtained {
			internal.Logger.Printf("redislock.Lock failed: %s", err)
		}
		if lock != nil {
			_ = lock.Release(ctx)
			lock = nil
		}

		timer.Reset(500 * time.Millisecond)
		select {
		case <-timer.C:
		case <-c.stopCh:
			if lock != nil {
				_ = lock.Release(ctx)
			}
			return nil
		}
	}
}

func (c *Consumer) String() string {
	fnum := atomic.LoadInt32(&c.numFetcher)
	wnum := atomic.LoadInt32(&c.numWorker)
	inFlight := atomic.LoadUint32(&c.inFlight)
	processed := atomic.LoadUint32(&c.processed)
	retries := atomic.LoadUint32(&c.retries)
	fails := atomic.LoadUint32(&c.fails)
	timing := c.timing()

	return fmt.Sprintf(
		"%s %d/%d %d/%d/%d %d/%d/%d %s",
		c.q.Name(),
		fnum, wnum,
		inFlight, len(c.buffer), cap(c.buffer),
		processed, retries, fails,
		timing)
}

func (c *Consumer) voteQueueEmpty() {
	c.changeQueueEmptyVote(+1)
}

func (c *Consumer) voteQueueFull() {
	c.changeQueueEmptyVote(-1)
}

func (c *Consumer) changeQueueEmptyVote(d int32) {
	const quorum = 7

	for i := 0; i < 100; i++ {
		n := atomic.LoadInt32(&c.queueEmptyVote)
		if (d < 0 && n <= -quorum) || (d > 0 && n >= quorum) {
			break
		}
		if atomic.CompareAndSwapInt32(&c.queueEmptyVote, n, n+d) {
			break
		}
	}
}

func (c *Consumer) queueEmpty() bool {
	return atomic.LoadInt32(&c.queueEmptyVote) >= 3
}

func (c *Consumer) autotune(ctx context.Context, cfg *consumerConfig) {
	timer := time.NewTimer(time.Hour)
	defer timer.Stop()

	for c.timing() == 0 {
		timer.Reset(250 * time.Millisecond)
		select {
		case <-timer.C:
			// continue
		case <-c.stopCh:
			return
		}
	}

	for {
		timer.Reset(c.autotuneInterval())
		select {
		case <-timer.C:
			cfg = c.autotuneTick(ctx, cfg)
		case <-c.stopCh:
			return
		}
	}
}

func (c *Consumer) autotuneInterval() time.Duration {
	const min = 500 * time.Millisecond
	const max = time.Minute

	d := 10 * c.timing()
	if d < min {
		return min
	}
	if d > max {
		return max
	}
	return d
}

func (c *Consumer) autotuneTick(ctx context.Context, cfg *consumerConfig) *consumerConfig {
	processed := int(atomic.LoadUint32(&c.processed))
	retries := int(atomic.LoadUint32(&c.retries))
	cfg.Update(processed, retries, c.timing())

	if newCfg := c.cfgs.Select(cfg, c.queueEmpty()); newCfg != nil {
		cfg = newCfg
		c.replaceConfig(ctx, cfg)
	}

	return cfg
}

func (c *Consumer) replaceConfig(ctx context.Context, cfg *consumerConfig) {
	if numFetcher := atomic.LoadInt32(&c.numFetcher); numFetcher != -1 {
		if numFetcher > cfg.NumFetcher {
			// Remove extra fetchers.
			atomic.StoreInt32(&c.numFetcher, cfg.NumFetcher)
		} else {
			for id := numFetcher; id < cfg.NumFetcher; id++ {
				if !c.addFetcher(ctx, id) {
					internal.Logger.Printf("taskq: addFetcher id=%d failed", id)
				}
			}
		}
	}

	numWorker := atomic.LoadInt32(&c.numWorker)
	if numWorker > cfg.NumWorker {
		// Remove extra workers.
		atomic.StoreInt32(&c.numWorker, cfg.NumWorker)
	} else {
		for id := numWorker; id < cfg.NumWorker; id++ {
			if !c.addWorker(ctx, id) {
				internal.Logger.Printf("taskq: addWorker id=%d failed", id)
			}
		}
	}

	cfg.Reset(
		int(atomic.LoadUint32(&c.processed)),
		int(atomic.LoadUint32(&c.retries)))
}

//------------------------------------------------------------------------------

type limiter struct {
	bucket  string
	limiter *redis_rate.Limiter
	limit   *redis_rate.Limit

	allowedCount uint32 // atomic
	cancelled    uint32 // atomic
}

func (l *limiter) Reserve(ctx context.Context, max int) int {
	if l.limiter == nil || l.limit == nil {
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

	for {
		res, err := l.limiter.AllowAtMost(ctx, l.bucket, l.limit, max)
		if err != nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		if res.Allowed > 0 {
			atomic.AddUint32(&l.allowedCount, 1)
			return res.Allowed
		}

		atomic.StoreUint32(&l.allowedCount, 0)
		time.Sleep(res.RetryAfter)
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
