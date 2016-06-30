package processor_test

import (
	"errors"
	"log"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	timerate "golang.org/x/time/rate"
	"gopkg.in/go-redis/rate.v4"
	"gopkg.in/redis.v4"

	"gopkg.in/queue.v1"
	"gopkg.in/queue.v1/processor"
)

func printStats(p *processor.Processor) {
	for _ = range time.Tick(3 * time.Second) {
		st := p.Stats()
		if st == nil {
			break
		}
		log.Printf(
			"%s: inFlight=%d deleting=%d processed=%d fails=%d retries=%d avg_dur=%s\n",
			p, st.InFlight, st.Deleting, st.Processed, st.Fails, st.Retries, st.AvgDuration,
		)
	}
}

func rateLimiter() *rate.Limiter {
	fallbackLimiter := timerate.NewLimiter(timerate.Every(time.Millisecond), 100)
	return rate.NewLimiter(redisRing(), fallbackLimiter)
}

type queueStorage struct {
	*redis.Ring
}

func (c queueStorage) Exists(key string) bool {
	return !c.SetNX(key, "", 12*time.Hour).Val()
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

func testProcessor(t *testing.T, q queue.Queuer) {
	_ = q.Purge()

	var count int64
	handler := func(hello, world string) error {
		if hello != "hello" {
			t.Fatalf("got %s, wanted hello", hello)
		}
		if world != "world" {
			t.Fatalf("got %s, wanted world", world)
		}
		atomic.AddInt64(&count, 1)
		return nil
	}

	msg := queue.NewMessage("hello", "world")
	err := q.Add(msg)
	if err != nil {
		t.Fatal(err)
	}

	p := processor.New(q, &queue.Options{
		Handler: handler,
	})
	p.Start()
	time.Sleep(time.Second)

	if err := p.Stop(); err != nil {
		t.Fatal(err)
	}
	if atomic.LoadInt64(&count) != 1 {
		t.Fatalf("message was not processed")
	}
}

func testDelay(t *testing.T, q queue.Queuer) {
	_ = q.Purge()

	handlerCh := make(chan time.Time, 10)
	handler := func() {
		handlerCh <- time.Now()
	}

	msg := queue.NewMessage()
	msg.Delay = 3 * time.Second
	err := q.Add(msg)
	if err != nil {
		t.Fatal(err)
	}

	p := processor.New(q, &queue.Options{
		Handler: handler,
	})
	start := time.Now()
	p.Start()

	tm := <-handlerCh
	sub := tm.Sub(start)
	if !durEqual(sub, msg.Delay) {
		t.Fatalf("message was delayed by %s, wanted %s", sub, msg.Delay)
	}

	if err := p.Stop(); err != nil {
		t.Fatal(err)
	}
}

func testRetry(t *testing.T, q queue.Queuer) {
	_ = q.Purge()

	handlerCh := make(chan bool, 10)
	handler := func(hello, world string) error {
		handlerCh <- true
		return errors.New("fake error")
	}

	msg := queue.NewMessage("hello", "world")
	err := q.Add(msg)
	if err != nil {
		t.Fatal(err)
	}

	p := processor.New(q, &queue.Options{
		Handler: handler,
		Retries: 3,
		Backoff: time.Second,
	})
	p.Start()

	timings := []time.Duration{0, time.Second, 3 * time.Second}
	testTimings(t, handlerCh, timings)

	if err := p.Stop(); err != nil {
		t.Fatal(err)
	}
}

func testNamedMessage(t *testing.T, q queue.Queuer) {
	_ = q.Purge()

	var count int64
	handler := func() error {
		atomic.AddInt64(&count, 1)
		return nil
	}

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			msg := queue.NewMessage()
			msg.Name = "the-name"
			err := q.Add(msg)
			if err != nil && err != queue.ErrDuplicate {
				t.Fatal(err)
			}
		}()
	}
	wg.Wait()

	p := processor.New(q, &queue.Options{
		Handler: handler,
	})
	p.Start()

	if err := p.Stop(); err != nil {
		t.Fatal(err)
	}

	if n := atomic.LoadInt64(&count); n != 1 {
		t.Fatalf("processed %d messages, wanted 1", n)
	}
}

func testRateLimit(t *testing.T, q queue.Queuer) {
	_ = q.Purge()

	var count int64
	handler := func() error {
		atomic.AddInt64(&count, 1)
		return nil
	}

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			msg := queue.NewMessage()
			err := q.Add(msg)
			if err != nil {
				t.Fatal(err)
			}
		}()
	}
	wg.Wait()

	p := processor.New(q, &queue.Options{
		Handler:   handler,
		Workers:   2,
		RateLimit: timerate.Every(time.Second),
		Limiter:   rateLimiter(),
	})
	p.Start()
	go printStats(p)

	time.Sleep(5 * time.Second)
	if n := atomic.LoadInt64(&count); n != 5 && n != 6 {
		t.Fatalf("processed %d messages, wanted 5", n)
	}

	if err := p.Stop(); err != nil {
		t.Fatal(err)
	}
}

type Error string

func (e Error) Error() string {
	return string(e)
}

func (Error) Delay() time.Duration {
	return 3 * time.Second
}

func testDelayer(t *testing.T, q queue.Queuer) {
	_ = q.Purge()

	handlerCh := make(chan bool, 10)
	handler := func() error {
		handlerCh <- true
		return Error("fake error")
	}

	p := processor.New(q, &queue.Options{
		Handler: handler,
		Backoff: time.Second,
	})
	p.Start()

	err := q.Call()
	if err != nil {
		t.Fatal(err)
	}

	timings := []time.Duration{0, 3 * time.Second, 3 * time.Second}
	testTimings(t, handlerCh, timings)

	if err := p.Stop(); err != nil {
		t.Fatal(err)
	}
}

func durEqual(d1, d2 time.Duration) bool {
	return d1 >= d2 && d2-d1 < 2*time.Second
}

func testTimings(t *testing.T, ch chan bool, timings []time.Duration) {
	start := time.Now()
	for i, timing := range timings {
		<-ch
		since := time.Since(start)
		if !durEqual(since, timing) {
			t.Fatalf("#%d: timing is %s, wanted %s", i+1, since, timing)
		}
	}
}
