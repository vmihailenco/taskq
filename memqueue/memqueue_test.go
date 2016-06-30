package memqueue_test

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"gopkg.in/redis.v4"

	"gopkg.in/queue.v1"
	"gopkg.in/queue.v1/memqueue"
	"gopkg.in/queue.v1/processor"
)

func TestMemqueue(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "memqueue")
}

var _ = Describe("message with args", func() {
	ch := make(chan bool, 10)
	handler := func(s string, i int) {
		Expect(s).To(Equal("string"))
		Expect(i).To(Equal(42))
		ch <- true
	}

	BeforeEach(func() {
		q := memqueue.NewMemqueue(&memqueue.Options{
			Processor: processor.Options{
				Handler: handler,
			},
		})
		q.CallAsync("string", 42)

		err := q.Close()
		Expect(err).NotTo(HaveOccurred())
	})

	It("handler is called with args", func() {
		Expect(ch).To(Receive())
		Expect(ch).NotTo(Receive())
	})
})

var _ = Describe("message with invalid number of args", func() {
	ch := make(chan bool, 10)
	handler := func(s string) {
		ch <- true
	}

	BeforeEach(func() {
		q := memqueue.NewMemqueue(&memqueue.Options{
			Processor: processor.Options{
				Handler: handler,

				Retries: 1,
			},
		})
		err := q.Call()
		Expect(err).To(MatchError("got 0 args, handler expects 1"))

		err = q.Close()
		Expect(err).NotTo(HaveOccurred())
	})

	It("handler is not called", func() {
		Expect(ch).NotTo(Receive())
	})
})

var _ = Describe("handler that expects Message", func() {
	ch := make(chan bool, 10)
	handler := func(msg *queue.Message) error {
		Expect(msg.Args).To(Equal([]interface{}{"string", 42}))
		ch <- true
		return nil
	}

	BeforeEach(func() {
		q := memqueue.NewMemqueue(&memqueue.Options{
			Processor: processor.Options{
				Handler: queue.HandlerFunc(handler),
			},
		})
		q.CallAsync("string", 42)

		err := q.Close()
		Expect(err).NotTo(HaveOccurred())
	})

	It("is called with Message", func() {
		Expect(ch).To(Receive())
		Expect(ch).NotTo(Receive())
	})
})

var _ = Describe("message retry timing", func() {
	var q *memqueue.Memqueue
	backoff := 100 * time.Millisecond

	var count int
	var ch chan time.Time
	handler := func() error {
		ch <- time.Now()
		count++
		return fmt.Errorf("fake error #%d", count)
	}

	BeforeEach(func() {
		count = 0
		ch = make(chan time.Time, 10)
		q = memqueue.NewMemqueue(&memqueue.Options{
			Processor: processor.Options{
				Handler: handler,
				Retries: 3,
				Backoff: backoff,
			},
		})
	})

	Context("without delay", func() {
		var now time.Time

		BeforeEach(func() {
			now = time.Now()

			q.CallAsync()

			err := q.Close()
			Expect(err).NotTo(HaveOccurred())
		})

		It("is retried in time", func() {
			Expect(ch).To(Receive(BeTemporally("~", now, backoff/10)))
			Expect(ch).To(Receive(BeTemporally("~", now.Add(backoff), backoff/10)))
			Expect(ch).To(Receive(BeTemporally("~", now.Add(3*backoff), backoff/10)))
			Expect(ch).NotTo(Receive())
		})
	})

	Context("message with delay", func() {
		var now time.Time

		BeforeEach(func() {
			msg := queue.NewMessage()
			msg.Delay = 5 * backoff
			now = time.Now().Add(msg.Delay)

			q.AddAsync(msg)

			err := q.Close()
			Expect(err).NotTo(HaveOccurred())
		})

		It("is retried in time", func() {
			Expect(ch).To(Receive(BeTemporally("~", now, backoff/10)))
			Expect(ch).To(Receive(BeTemporally("~", now.Add(backoff), backoff/10)))
			Expect(ch).To(Receive(BeTemporally("~", now.Add(3*backoff), backoff/10)))
			Expect(ch).NotTo(Receive())
		})
	})

	Context("wrapped message with delay", func() {
		BeforeEach(func() {
			err := q.Close()
			Expect(err).NotTo(HaveOccurred())

			q = memqueue.NewMemqueue(&memqueue.Options{
				Processor: processor.Options{
					Handler: handler,
					Retries: 3,
					Backoff: backoff,
				},
			})
		})

		It("is processed immediately with async API", func() {
			msg := queue.NewMessage()
			msg.Delay = time.Hour
			msg.Wrapped = true
			err := q.AddAsync(msg)
			Expect(err).NotTo(HaveOccurred())
			now := time.Now()

			err = q.Close()
			Expect(err).NotTo(HaveOccurred())

			Expect(ch).To(Receive(BeTemporally("~", now, backoff/10)))
			Expect(ch).To(Receive(BeTemporally("~", now.Add(backoff), backoff/10)))
			Expect(ch).To(Receive(BeTemporally("~", now.Add(3*backoff), backoff/10)))
			Expect(ch).NotTo(Receive())
		})

		It("is processed immediately with sync API", func() {
			now := time.Now()

			msg := queue.NewMessage()
			msg.Delay = time.Hour
			msg.Wrapped = true
			err := q.Add(msg)
			Expect(err).To(MatchError("fake error #3"))

			err = q.Close()
			Expect(err).NotTo(HaveOccurred())

			Expect(ch).To(Receive(BeTemporally("~", now, backoff/10)))
			Expect(ch).To(Receive(BeTemporally("~", now.Add(backoff), backoff/10)))
			Expect(ch).To(Receive(BeTemporally("~", now.Add(3*backoff), backoff/10)))
			Expect(ch).NotTo(Receive())
		})
	})
})

var _ = Describe("failing queue with error handler", func() {
	var q *memqueue.Memqueue

	handler := func() error {
		return errors.New("fake error")
	}

	ch := make(chan bool, 10)
	fallbackHandler := func() {
		ch <- true
	}

	BeforeEach(func() {
		q = memqueue.NewMemqueue(&memqueue.Options{
			Processor: processor.Options{
				Handler:         handler,
				FallbackHandler: fallbackHandler,
				Retries:         1,
			},
		})
		q.CallAsync()

		err := q.Close()
		Expect(err).NotTo(HaveOccurred())
	})

	It("error handler is called when handler fails", func() {
		Expect(ch).To(Receive())
		Expect(ch).NotTo(Receive())
	})
})

type memqueueStorage struct {
	*redis.Ring
}

func (c memqueueStorage) Exists(key string) bool {
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

var _ = Describe("named message", func() {
	var count int64
	handler := func() {
		atomic.AddInt64(&count, 1)
	}

	BeforeEach(func() {
		q := memqueue.NewMemqueue(&memqueue.Options{
			Storage: memqueueStorage{redisRing()},
			Processor: processor.Options{
				Handler: handler,
			},
		})

		var wg sync.WaitGroup
		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func() {
				defer GinkgoRecover()
				defer wg.Done()
				msg := queue.NewMessage()
				msg.Name = "myname"
				q.AddAsync(msg)
			}()
		}
		wg.Wait()

		err := q.Close()
		Expect(err).NotTo(HaveOccurred())
	})

	It("is processed once", func() {
		n := atomic.LoadInt64(&count)
		Expect(n).To(Equal(int64(1)))
	})
})

// slot splits time into equal periods (called slots) and returns
// slot number for provided time.
func slot(period time.Duration) int64 {
	tm := time.Now()
	periodSec := int64(period / time.Second)
	if periodSec == 0 {
		return tm.Unix()
	}
	return tm.Unix() / periodSec
}

var _ = Describe("CallOnce", func() {
	var now time.Time
	delay := 200 * time.Millisecond

	ch := make(chan time.Time, 10)
	handler := func(slot int64) error {
		ch <- time.Now()
		return nil
	}

	BeforeEach(func() {
		now = time.Now()

		q := memqueue.NewMemqueue(&memqueue.Options{
			Storage: memqueueStorage{redisRing()},
			Processor: processor.Options{
				Handler: handler,
			},
		})

		var wg sync.WaitGroup
		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func() {
				defer GinkgoRecover()
				defer wg.Done()

				q.CallOnceAsync(delay, slot(delay))
			}()
		}
		wg.Wait()

		err := q.Close()
		Expect(err).NotTo(HaveOccurred())
	})

	It("processes message once with delay", func() {
		Expect(ch).To(Receive(BeTemporally("~", now.Add(delay), delay/10)))
		Consistently(ch).ShouldNot(Receive())
	})
})

var _ = Describe("stress testing", func() {
	var q *memqueue.Memqueue
	const n = 10000

	var count int64
	handler := func() {
		atomic.AddInt64(&count, 1)
	}

	BeforeEach(func() {
		q = memqueue.NewMemqueue(&memqueue.Options{
			Processor: processor.Options{
				Handler: handler,
			},
		})

		for i := 0; i < n; i++ {
			q.CallAsync()
		}

		err := q.Close()
		Expect(err).NotTo(HaveOccurred())
	})

	It("handler is called for all messages", func() {
		nn := atomic.LoadInt64(&count)
		Expect(nn).To(Equal(int64(n)))
	})
})

var _ = Describe("stress testing failing queue", func() {
	var q *memqueue.Memqueue
	const n = 10000

	handler := func() error {
		return errors.New("fake error")
	}

	var errorCount int64
	fallbackHandler := func() {
		atomic.AddInt64(&errorCount, 1)
	}

	BeforeEach(func() {
		q = memqueue.NewMemqueue(&memqueue.Options{
			Processor: processor.Options{
				Handler:         handler,
				FallbackHandler: fallbackHandler,
				Retries:         1,
			},
		})

		for i := 0; i < n; i++ {
			q.CallAsync()
		}

		err := q.Close()
		Expect(err).NotTo(HaveOccurred())
	})

	It("error handler is called for all messages", func() {
		nn := atomic.LoadInt64(&errorCount)
		Expect(nn).To(Equal(int64(n)))
	})
})

func BenchmarkCallAsync(b *testing.B) {
	q := memqueue.NewMemqueue(&memqueue.Options{
		Processor: processor.Options{
			Handler:    func() {},
			BufferSize: 1000000,
		},
	})
	defer q.Close()

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			q.CallAsync()
		}
	})
}

func BenchmarkNamedMessage(b *testing.B) {
	q := memqueue.NewMemqueue(&memqueue.Options{
		Storage: memqueueStorage{redisRing()},
		Processor: processor.Options{
			Handler:    func() {},
			BufferSize: 1000000,
		},
	})
	defer q.Close()

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			msg := queue.NewMessage()
			msg.Name = "myname"
			q.AddAsync(msg)
		}
	})
}
