package taskq_test

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-redis/redis"
	"golang.org/x/time/rate"

	"github.com/vmihailenco/taskq/v2"
)

const waitTimeout = time.Second
const testTimeout = 30 * time.Second

func queueName(s string) string {
	version := strings.Split(runtime.Version(), " ")[0]
	version = strings.Replace(version, ".", "", -1)
	return "test-" + s + "-" + version
}

var (
	ringOnce sync.Once
	ring     *redis.Ring
)

func redisRing() *redis.Ring {
	ringOnce.Do(func() {
		ring = redis.NewRing(&redis.RingOptions{
			Addrs: map[string]string{"0": ":6379"},
		})
	})
	_ = ring.FlushDB().Err()
	return ring
}

func testConsumer(t *testing.T, factory taskq.Factory, opt *taskq.QueueOptions) {
	c := context.Background()
	opt.WaitTimeout = waitTimeout
	opt.Redis = redisRing()

	q := factory.RegisterQueue(opt)
	purge(t, q)

	ch := make(chan time.Time)
	task := taskq.RegisterTask(&taskq.TaskOptions{
		Name: nextTaskID(),
		Handler: func(hello, world string) error {
			if hello != "hello" {
				t.Fatalf("got %s, wanted hello", hello)
			}
			if world != "world" {
				t.Fatalf("got %s, wanted world", world)
			}
			ch <- time.Now()
			return nil
		},
	})

	err := q.Add(task.WithArgs(c, "hello", "world"))
	if err != nil {
		t.Fatal(err)
	}

	p := q.Consumer()
	if err := p.Start(c); err != nil {
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

func testUnknownTask(t *testing.T, factory taskq.Factory, opt *taskq.QueueOptions) {
	c := context.Background()
	opt.WaitTimeout = waitTimeout
	opt.Redis = redisRing()

	q := factory.RegisterQueue(opt)
	purge(t, q)

	_ = taskq.RegisterTask(&taskq.TaskOptions{
		Name:    nextTaskID(),
		Handler: func() {},
	})

	taskq.SetUnknownTaskOptions(&taskq.TaskOptions{
		Name:       "unknown",
		RetryLimit: 1,
	})

	msg := taskq.NewMessage(c)
	msg.TaskName = "unknown"
	err := q.Add(msg)
	if err != nil {
		t.Fatal(err)
	}

	p := q.Consumer()
	if err := p.Start(c); err != nil {
		t.Fatal(err)
	}

	if err := p.Stop(); err != nil {
		t.Fatal(err)
	}

	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
}

func testFallback(t *testing.T, factory taskq.Factory, opt *taskq.QueueOptions) {
	c := context.Background()
	opt.WaitTimeout = waitTimeout
	opt.Redis = redisRing()

	q := factory.RegisterQueue(opt)
	purge(t, q)

	ch := make(chan time.Time)
	task := taskq.RegisterTask(&taskq.TaskOptions{
		Name: nextTaskID(),
		Handler: func() error {
			return errors.New("fake error")
		},
		FallbackHandler: func(hello, world string) error {
			if hello != "hello" {
				t.Fatalf("got %s, wanted hello", hello)
			}
			if world != "world" {
				t.Fatalf("got %s, wanted world", world)
			}
			ch <- time.Now()
			return nil
		},
		RetryLimit: 1,
	})

	err := q.Add(task.WithArgs(c, "hello", "world"))
	if err != nil {
		t.Fatal(err)
	}

	p := q.Consumer()
	p.Start(c)

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

func testDelay(t *testing.T, factory taskq.Factory, opt *taskq.QueueOptions) {
	c := context.Background()
	opt.WaitTimeout = waitTimeout
	opt.Redis = redisRing()

	q := factory.RegisterQueue(opt)
	purge(t, q)

	handlerCh := make(chan time.Time, 10)
	task := taskq.RegisterTask(&taskq.TaskOptions{
		Name: nextTaskID(),
		Handler: func() {
			handlerCh <- time.Now()
		},
	})

	start := time.Now()

	msg := task.WithArgs(c)
	msg.Delay = 5 * time.Second
	err := q.Add(msg)
	if err != nil {
		t.Fatal(err)
	}

	p := q.Consumer()
	p.Start(c)

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

func testRetry(t *testing.T, factory taskq.Factory, opt *taskq.QueueOptions) {
	c := context.Background()
	opt.WaitTimeout = waitTimeout
	opt.Redis = redisRing()

	q := factory.RegisterQueue(opt)
	purge(t, q)

	handlerCh := make(chan time.Time, 10)
	task := taskq.RegisterTask(&taskq.TaskOptions{
		Name: nextTaskID(),
		Handler: func(hello, world string) error {
			if hello != "hello" {
				t.Fatalf("got %q, wanted hello", hello)
			}
			if world != "world" {
				t.Fatalf("got %q, wanted world", world)
			}
			handlerCh <- time.Now()
			return errors.New("fake error")
		},
		FallbackHandler: func(msg *taskq.Message) error {
			handlerCh <- time.Now()
			return nil
		},
		RetryLimit: 3,
		MinBackoff: time.Second,
	})

	err := q.Add(task.WithArgs(c, "hello", "world"))
	if err != nil {
		t.Fatal(err)
	}

	p := q.Consumer()
	p.Start(c)

	timings := []time.Duration{0, time.Second, 3 * time.Second, 3 * time.Second}
	testTimings(t, handlerCh, timings)

	if err := p.Stop(); err != nil {
		t.Fatal(err)
	}

	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
}

func testNamedMessage(t *testing.T, factory taskq.Factory, opt *taskq.QueueOptions) {
	c := context.Background()
	opt.WaitTimeout = waitTimeout
	opt.Redis = redisRing()

	q := factory.RegisterQueue(opt)
	purge(t, q)

	ch := make(chan time.Time, 10)
	task := taskq.RegisterTask(&taskq.TaskOptions{
		Name: nextTaskID(),
		Handler: func(hello string) error {
			if hello != "world" {
				panic("hello != world")
			}
			ch <- time.Now()
			return nil
		},
	})

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			msg := task.WithArgs(c, "world")
			msg.Name = "the-name"
			err := q.Add(msg)
			if err != nil && err != taskq.ErrDuplicate {
				t.Fatal(err)
			}
		}()
	}
	wg.Wait()

	p := q.Consumer()
	p.Start(c)

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

func testCallOnce(t *testing.T, factory taskq.Factory, opt *taskq.QueueOptions) {
	c := context.Background()
	opt.WaitTimeout = waitTimeout
	opt.Redis = redisRing()

	q := factory.RegisterQueue(opt)
	purge(t, q)

	ch := make(chan time.Time, 10)
	task := taskq.RegisterTask(&taskq.TaskOptions{
		Name: nextTaskID(),
		Handler: func() {
			ch <- time.Now()
		},
	})

	go func() {
		for i := 0; i < 3; i++ {
			for j := 0; j < 10; j++ {
				err := q.Add(task.WithArgs(c).OnceInPeriod(500 * time.Millisecond))
				if err != nil && err != taskq.ErrDuplicate {
					t.Fatal(err)
				}
			}
			time.Sleep(time.Second)
		}
	}()

	p := q.Consumer()
	if err := p.Start(c); err != nil {
		t.Fatal(err)
	}

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
	case <-time.After(time.Second):
	}

	if err := p.Stop(); err != nil {
		t.Fatal(err)
	}

	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
}

func testLen(t *testing.T, factory taskq.Factory, opt *taskq.QueueOptions) {
	const N = 10

	c := context.Background()
	opt.WaitTimeout = waitTimeout
	opt.Redis = redisRing()

	q := factory.RegisterQueue(opt)
	purge(t, q)

	task := taskq.RegisterTask(&taskq.TaskOptions{
		Name:    nextTaskID(),
		Handler: func() {},
	})

	for i := 0; i < N; i++ {
		err := q.Add(task.WithArgs(c))
		if err != nil {
			t.Fatal(err)
		}
	}

	eventually(func() error {
		n, err := q.Len()
		if err != nil {
			return err
		}

		if n != N {
			return fmt.Errorf("got %d messages, wanted %d", n, N)
		}
		return nil
	}, testTimeout)

	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
}

func testRateLimit(t *testing.T, factory taskq.Factory, opt *taskq.QueueOptions) {
	c := context.Background()
	opt.WaitTimeout = waitTimeout
	opt.RateLimit = rate.Every(time.Second)
	opt.Redis = redisRing()

	q := factory.RegisterQueue(opt)
	purge(t, q)

	var count int64
	task := taskq.RegisterTask(&taskq.TaskOptions{
		Name: nextTaskID(),
		Handler: func() {
			atomic.AddInt64(&count, 1)
		},
	})

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			err := q.Add(task.WithArgs(c))
			if err != nil {
				t.Fatal(err)
			}
		}()
	}
	wg.Wait()

	p := q.Consumer()
	p.Start(c)

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

func testErrorDelay(t *testing.T, factory taskq.Factory, opt *taskq.QueueOptions) {
	c := context.Background()
	opt.WaitTimeout = waitTimeout
	opt.Redis = redisRing()

	q := factory.RegisterQueue(opt)
	purge(t, q)

	handlerCh := make(chan time.Time, 10)
	task := taskq.RegisterTask(&taskq.TaskOptions{
		Name: nextTaskID(),
		Handler: func() error {
			handlerCh <- time.Now()
			return RateLimitError("fake error")
		},
		MinBackoff: time.Second,
		RetryLimit: 3,
	})

	err := q.Add(task.WithArgs(c))
	if err != nil {
		t.Fatal(err)
	}

	p := q.Consumer()
	p.Start(c)

	timings := []time.Duration{0, 3 * time.Second, 3 * time.Second}
	testTimings(t, handlerCh, timings)

	if err := p.Stop(); err != nil {
		t.Fatal(err)
	}

	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
}

func testWorkerLimit(t *testing.T, factory taskq.Factory, opt *taskq.QueueOptions) {
	ctx := context.Background()
	opt.WaitTimeout = waitTimeout
	opt.Redis = redisRing()
	opt.WorkerLimit = 1

	q := factory.RegisterQueue(opt)
	purge(t, q)

	ch := make(chan time.Time, 10)
	task := taskq.RegisterTask(&taskq.TaskOptions{
		Name: nextTaskID(),
		Handler: func() {
			ch <- time.Now()
			time.Sleep(time.Second)
		},
	})

	for i := 0; i < 3; i++ {
		err := q.Add(task.WithArgs(ctx))
		if err != nil {
			t.Fatal(err)
		}
	}

	p1 := taskq.StartConsumer(ctx, q)
	p2 := taskq.StartConsumer(ctx, q)

	timings := []time.Duration{0, time.Second, 2 * time.Second}
	testTimings(t, ch, timings)

	if err := p1.Stop(); err != nil {
		t.Fatal(err)
	}
	if err := p2.Stop(); err != nil {
		t.Fatal(err)
	}
}

func testInvalidCredentials(t *testing.T, factory taskq.Factory, opt *taskq.QueueOptions) {
	ctx := context.Background()
	opt.WaitTimeout = waitTimeout
	opt.Redis = redisRing()

	q := factory.RegisterQueue(opt)

	ch := make(chan time.Time, 10)
	task := taskq.RegisterTask(&taskq.TaskOptions{
		Name: nextTaskID(),
		Handler: func(s1, s2 string) {
			if s1 != "hello" {
				t.Fatalf("got %q, wanted hello", s1)
			}
			if s2 != "world" {
				t.Fatalf("got %q, wanted world", s1)
			}
			ch <- time.Now()
		},
	})

	err := q.Add(task.WithArgs(ctx, "hello", "world"))
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

func testBatchConsumer(
	t *testing.T, factory taskq.Factory, opt *taskq.QueueOptions, messageSize int,
) {
	const N = 16

	ctx := context.Background()
	opt.WaitTimeout = waitTimeout
	opt.Redis = redisRing()

	payload := make([]byte, messageSize)
	_, err := rand.Read(payload)
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	wg.Add(N)

	opt.WaitTimeout = waitTimeout
	q := factory.RegisterQueue(opt)
	purge(t, q)

	task := taskq.RegisterTask(&taskq.TaskOptions{
		Name: nextTaskID(),
		Handler: func(s string) {
			defer wg.Done()
			if s != string(payload) {
				t.Fatalf("s != payload")
			}
		},
	})

	for i := 0; i < N; i++ {
		err := q.Add(task.WithArgs(ctx, payload))
		if err != nil {
			t.Fatal(err)
		}
	}

	p := q.Consumer()
	if err := p.Start(ctx); err != nil {
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

func purge(t *testing.T, q taskq.Queue) {
	err := q.Purge()
	if err == nil {
		return
	}

	task := taskq.RegisterTask(&taskq.TaskOptions{
		Name:    "*",
		Handler: func() {},
	})

	consumer := taskq.NewConsumer(q)
	err = consumer.ProcessAll(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	taskq.Tasks.Unregister(task)
}

func eventually(fn func() error, timeout time.Duration) error {
	errCh := make(chan error)
	done := make(chan struct{})
	exit := make(chan struct{})

	go func() {
		for {
			err := fn()
			if err == nil {
				close(done)
				return
			}

			select {
			case errCh <- err:
			default:
			}

			select {
			case <-exit:
				return
			case <-time.After(timeout / 100):
			}
		}
	}()

	select {
	case <-done:
		return nil
	case <-time.After(timeout):
		close(exit)
		select {
		case err := <-errCh:
			return err
		default:
			return fmt.Errorf("timeout after %s", timeout)
		}
	}
}

var taskID int

func nextTaskID() string {
	id := strconv.Itoa(taskID)
	taskID++
	return id
}
