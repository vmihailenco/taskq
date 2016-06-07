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
	ring := redis.NewRing(&redis.RingOptions{
		Addrs: map[string]string{"server0": ":6379"},
	})
	if err := ring.FlushDb().Err(); err != nil {
		panic(err)
	}
	fallbackLimiter := timerate.NewLimiter(timerate.Every(time.Millisecond), 100)
	return rate.NewLimiter(ring, fallbackLimiter)
}

func testProcessor(t *testing.T, q processor.Queuer) {
	if err := q.Purge(); err != nil {
		t.Fatal(err)
	}

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

	p := processor.New(q, &processor.Options{
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

func testDelay(t *testing.T, q processor.Queuer) {
	if err := q.Purge(); err != nil {
		t.Fatal(err)
	}

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

	p := processor.New(q, &processor.Options{
		Handler: handler,
	})
	start := time.Now()
	p.Start()

	tm := <-handlerCh
	timing := tm.Sub(start)
	if timing < msg.Delay || msg.Delay-timing > 2*time.Second {
		t.Fatalf("message was delayed by %s, wanted %s", timing, msg.Delay)
	}

	if err := p.Stop(); err != nil {
		t.Fatal(err)
	}
}

func testRetry(t *testing.T, q processor.Queuer) {
	if err := q.Purge(); err != nil {
		t.Fatal(err)
	}

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

	p := processor.New(q, &processor.Options{
		Handler: handler,
	})
	start := time.Now()
	p.Start()

	timings := []time.Duration{0, time.Second, 3 * time.Second}
	for i, timing := range timings {
		<-handlerCh
		since := time.Since(start)
		if since < timing {
			t.Fatalf("retry %d: message was delayed by %s, wanted %s", i+1, since, timing)
		}
	}

	if err := p.Stop(); err != nil {
		t.Fatal(err)
	}
}

func testRateLimit(t *testing.T, q processor.Queuer) {
	if err := q.Purge(); err != nil {
		t.Fatal(err)
	}

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

	p := processor.New(q, &processor.Options{
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
