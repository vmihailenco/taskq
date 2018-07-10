package msgqueue_test

import (
	"errors"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-redis/redis"
	"golang.org/x/time/rate"

	"github.com/go-msgqueue/msgqueue"
	"github.com/go-msgqueue/msgqueue/internal"
)

const waitTimeout = time.Second
const testTimeout = 30 * time.Second

func queueName(s string) string {
	version := strings.Split(runtime.Version(), " ")[0]
	version = strings.Replace(version, ".", "", -1)
	return "test-" + s + "-" + version
}

func printStats(p *msgqueue.Processor) {
	q := p.Queue()

	var old *msgqueue.ProcessorStats
	for _ = range time.Tick(3 * time.Second) {
		st := p.Stats()
		if st == nil {
			break
		}

		if old != nil && st.Processed == old.Processed &&
			st.Fails == old.Fails &&
			st.Retries == old.Retries {
			continue
		}
		old = st

		internal.Logf(
			"%s: fetchers=%d buffered=%d/%d "+
				"in_flight=%d/%d "+
				"processed=%d fails=%d retries=%d "+
				"avg_dur=%s min_dur=%s max_dur=%s",
			q, st.FetcherNumber, st.Buffered, st.BufferSize,
			st.InFlight, st.WorkerNumber,
			st.Processed, st.Fails, st.Retries,
			st.AvgDuration, st.MinDuration, st.MaxDuration,
		)
	}
}

func redisRing() *redis.Ring {
	ring := redis.NewRing(&redis.RingOptions{
		Addrs:    map[string]string{"0": ":6379"},
		PoolSize: 100,
	})
	err := ring.FlushDb().Err()
	if err != nil {
		panic(err)
	}
	return ring
}

func testProcessor(t *testing.T, man msgqueue.Manager, opt *msgqueue.Options) {
	t.Parallel()

	ch := make(chan time.Time)
	opt.Handler = func(hello, world string) error {
		if hello != "hello" {
			t.Fatalf("got %s, wanted hello", hello)
		}
		if world != "world" {
			t.Fatalf("got %s, wanted world", world)
		}
		ch <- time.Now()
		return nil
	}

	opt.WaitTimeout = waitTimeout

	q := man.NewQueue(opt)
	purge(t, q)

	msg := msgqueue.NewMessage("hello", "world")
	err := q.Add(msg)
	if err != nil {
		t.Fatal(err)
	}

	p := q.Processor()
	if err := p.Start(); err != nil {
		t.Fatal(err)
	}

	select {
	case <-ch:
	case <-time.After(testTimeout):
		t.Fatalf("message was not processed")
	}

	if err := p.Stop(); err != nil {
		t.Fatal(err)
	}

	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
}

func testFallback(t *testing.T, man msgqueue.Manager, opt *msgqueue.Options) {
	t.Parallel()

	opt.Handler = func() error {
		return errors.New("fake error")
	}

	ch := make(chan time.Time)
	opt.FallbackHandler = func(hello, world string) error {
		if hello != "hello" {
			t.Fatalf("got %s, wanted hello", hello)
		}
		if world != "world" {
			t.Fatalf("got %s, wanted world", world)
		}
		ch <- time.Now()
		return nil
	}

	opt.WaitTimeout = waitTimeout
	opt.RetryLimit = 1
	opt.WaitTimeout = waitTimeout

	q := man.NewQueue(opt)
	purge(t, q)

	msg := msgqueue.NewMessage("hello", "world")
	err := q.Add(msg)
	if err != nil {
		t.Fatal(err)
	}

	p := q.Processor()
	p.Start()

	select {
	case <-ch:
	case <-time.After(testTimeout):
		t.Fatalf("message was not processed")
	}

	if err := p.Stop(); err != nil {
		t.Fatal(err)
	}

	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
}

func testDelay(t *testing.T, man msgqueue.Manager, opt *msgqueue.Options) {
	t.Parallel()

	handlerCh := make(chan time.Time, 10)
	opt.Handler = func() {
		handlerCh <- time.Now()
	}

	opt.WaitTimeout = waitTimeout

	q := man.NewQueue(opt)
	purge(t, q)

	start := time.Now()

	msg := msgqueue.NewMessage()
	msg.Delay = 5 * time.Second
	err := q.Add(msg)
	if err != nil {
		t.Fatal(err)
	}

	p := q.Processor()
	p.Start()

	var tm time.Time
	select {
	case tm = <-handlerCh:
	case <-time.After(testTimeout):
		t.Fatalf("message was not processed")
	}

	sub := tm.Sub(start)
	if !durEqual(sub, msg.Delay) {
		t.Fatalf("message was delayed by %s, wanted %s", sub, msg.Delay)
	}

	if err := p.Stop(); err != nil {
		t.Fatal(err)
	}

	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
}

func testRetry(t *testing.T, man msgqueue.Manager, opt *msgqueue.Options) {
	t.Parallel()

	handlerCh := make(chan time.Time, 10)
	opt.Handler = func(hello, world string) error {
		if hello != "hello" {
			t.Fatalf("got %q, wanted hello", hello)
		}
		if world != "world" {
			t.Fatalf("got %q, wanted world", world)
		}
		handlerCh <- time.Now()
		return errors.New("fake error")
	}

	opt.FallbackHandler = func() error {
		handlerCh <- time.Now()
		return nil
	}

	opt.WaitTimeout = waitTimeout
	opt.RetryLimit = 3
	opt.MinBackoff = time.Second

	q := man.NewQueue(opt)
	purge(t, q)

	msg := msgqueue.NewMessage("hello", "world")
	err := q.Add(msg)
	if err != nil {
		t.Fatal(err)
	}

	p := q.Processor()
	p.Start()

	timings := []time.Duration{0, time.Second, 3 * time.Second, 3 * time.Second}
	testTimings(t, handlerCh, timings)

	if err := p.Stop(); err != nil {
		t.Fatal(err)
	}

	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
}

func testNamedMessage(t *testing.T, man msgqueue.Manager, opt *msgqueue.Options) {
	t.Parallel()

	ch := make(chan time.Time, 10)
	opt.Handler = func(hello string) error {
		if hello != "world" {
			panic("hello != world")
		}
		ch <- time.Now()
		return nil
	}

	opt.WaitTimeout = waitTimeout
	opt.Redis = redisRing()

	q := man.NewQueue(opt)
	purge(t, q)

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			msg := msgqueue.NewMessage("world")
			msg.Name = "the-name"
			err := q.Add(msg)
			if err != nil && err != msgqueue.ErrDuplicate {
				t.Fatal(err)
			}
		}()
	}
	wg.Wait()

	p := q.Processor()
	p.Start()

	select {
	case <-ch:
	case <-time.After(testTimeout):
		t.Fatalf("message was not processed")
	}

	select {
	case <-ch:
		t.Fatalf("message was processed twice")
	default:
	}

	if err := p.Stop(); err != nil {
		t.Fatal(err)
	}

	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
}

func testCallOnce(t *testing.T, man msgqueue.Manager, opt *msgqueue.Options) {
	t.Parallel()

	ch := make(chan time.Time, 10)
	opt.Handler = func() {
		ch <- time.Now()
	}

	opt.WaitTimeout = waitTimeout
	opt.Redis = redisRing()

	q := man.NewQueue(opt)
	purge(t, q)

	go func() {
		for i := 0; i < 3; i++ {
			for j := 0; j < 10; j++ {
				err := q.CallOnce(500 * time.Millisecond)
				if err != nil && err != msgqueue.ErrDuplicate {
					t.Fatal(err)
				}
			}
			time.Sleep(time.Second)
		}
	}()

	p := q.Processor()
	p.Start()

	for i := 0; i < 3; i++ {
		select {
		case <-ch:
		case <-time.After(testTimeout):
			t.Fatalf("message was not processed")
		}
	}

	select {
	case <-ch:
		t.Fatalf("message was processed twice")
	default:
	}

	if err := p.Stop(); err != nil {
		t.Fatal(err)
	}

	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
}

func testLen(t *testing.T, man msgqueue.Manager, opt *msgqueue.Options) {
	t.Parallel()

	q := man.NewQueue(opt)
	purge(t, q)

	nmessages := 10
	for i := 0; i < nmessages; i++ {
		err := q.Call()
		if err != nil {
			t.Fatal(err)
		}
	}

	time.Sleep(time.Second)

	i, err := q.Len()
	if err != nil {
		t.Fatal(err)
	}
	if i != nmessages {
		t.Fatalf("got %d messages, wanted %d", i, nmessages)
	}
}

func testRateLimit(t *testing.T, man msgqueue.Manager, opt *msgqueue.Options) {
	t.Parallel()

	var count int64
	opt.Handler = func() {
		atomic.AddInt64(&count, 1)
	}

	opt.WaitTimeout = waitTimeout
	opt.RateLimit = rate.Every(time.Second)
	opt.Redis = redisRing()

	q := man.NewQueue(opt)
	purge(t, q)

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			msg := msgqueue.NewMessage()
			err := q.Add(msg)
			if err != nil {
				t.Fatal(err)
			}
		}()
	}
	wg.Wait()

	p := q.Processor()
	p.Start()

	time.Sleep(5 * time.Second)

	if n := atomic.LoadInt64(&count); n-5 > 2 {
		t.Fatalf("processed %d messages, wanted 5", n)
	}

	if err := p.Stop(); err != nil {
		t.Fatal(err)
	}

	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
}

func testErrorDelay(t *testing.T, man msgqueue.Manager, opt *msgqueue.Options) {
	t.Parallel()

	handlerCh := make(chan time.Time, 10)
	opt.Handler = func() error {
		handlerCh <- time.Now()
		return RateLimitError("fake error")
	}

	opt.WaitTimeout = waitTimeout
	opt.MinBackoff = time.Second
	opt.RetryLimit = 3

	q := man.NewQueue(opt)
	purge(t, q)

	err := q.Call()
	if err != nil {
		t.Fatal(err)
	}

	p := q.Processor()
	p.Start()

	timings := []time.Duration{0, 3 * time.Second, 3 * time.Second}
	testTimings(t, handlerCh, timings)

	if err := p.Stop(); err != nil {
		t.Fatal(err)
	}

	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
}

func testWorkerLimit(t *testing.T, man msgqueue.Manager, opt *msgqueue.Options) {
	t.Parallel()

	ch := make(chan time.Time, 10)
	opt.Handler = func() {
		ch <- time.Now()
		time.Sleep(time.Second)
	}

	opt.WaitTimeout = waitTimeout
	opt.Redis = redisRing()
	opt.WorkerLimit = 1

	q := man.NewQueue(opt)
	purge(t, q)

	for i := 0; i < 3; i++ {
		err := q.Call()
		if err != nil {
			t.Fatal(err)
		}
	}

	p1 := msgqueue.StartProcessor(q, opt)
	p2 := msgqueue.StartProcessor(q, opt)

	timings := []time.Duration{0, time.Second, 2 * time.Second}
	testTimings(t, ch, timings)

	if err := p1.Stop(); err != nil {
		t.Fatal(err)
	}
	if err := p2.Stop(); err != nil {
		t.Fatal(err)
	}
}

func testInvalidCredentials(t *testing.T, man msgqueue.Manager, opt *msgqueue.Options) {
	t.Parallel()

	ch := make(chan time.Time, 10)
	opt.Handler = func(s1, s2 string) {
		if s1 != "hello" {
			t.Fatalf("got %q, wanted hello", s1)
		}
		if s2 != "world" {
			t.Fatalf("got %q, wanted world", s1)
		}
		ch <- time.Now()
	}

	q := man.NewQueue(opt)

	err := q.Call("hello", "world")
	if err != nil {
		t.Fatal(err)
	}

	timings := []time.Duration{3 * time.Second}
	testTimings(t, ch, timings)

	err = q.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func testBatchProcessor(
	t *testing.T, man msgqueue.Manager, opt *msgqueue.Options, messageSize int,
) {
	t.Parallel()

	const N = 16
	payload := strings.Repeat("x", messageSize)

	var wg sync.WaitGroup
	wg.Add(N)

	opt.Handler = func(s string) {
		defer wg.Done()
		if s != payload {
			t.Fatalf("s != largeStr")
		}
	}
	opt.WaitTimeout = waitTimeout

	q := man.NewQueue(opt)
	purge(t, q)

	for i := 0; i < N; i++ {
		err := q.Call(payload)
		if err != nil {
			t.Fatal(err)
		}
	}

	p := q.Processor()
	if err := p.Start(); err != nil {
		t.Fatal(err)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(testTimeout):
		t.Fatalf("messages were not processed")
	}

	if err := p.Stop(); err != nil {
		t.Fatal(err)
	}

	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
}

func durEqual(d1, d2 time.Duration) bool {
	return d1 >= d2 && d2-d1 < 3*time.Second
}

func testTimings(t *testing.T, ch chan time.Time, timings []time.Duration) {
	start := time.Now()
	for i, timing := range timings {
		var tm time.Time
		select {
		case tm = <-ch:
		case <-time.After(testTimeout):
			t.Fatalf("message is not processed after %s", 2*timing)
		}
		since := tm.Sub(start)
		if !durEqual(since, timing) {
			t.Fatalf("#%d: timing is %s, wanted %s", i+1, since, timing)
		}
	}
}

func purge(t *testing.T, q msgqueue.Queue) {
	err := q.Purge()
	if err == nil {
		return
	}

	p := msgqueue.NewProcessor(q, &msgqueue.Options{
		Handler: func() {},
	})
	err = p.ProcessAll()
	if err != nil {
		t.Fatal(err)
	}
}
