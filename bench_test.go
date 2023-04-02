package taskq_test

import (
	"context"
	"sync"
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/vmihailenco/taskq/v4"
	"github.com/vmihailenco/taskq/v4/memqueue"
)

func BenchmarkConsumerMemq(b *testing.B) {
	benchmarkConsumer(b, memqueue.NewFactory())
}

var (
	once sync.Once
	q    taskq.Queue
	task *taskq.Task
	wg   sync.WaitGroup
)

func benchmarkConsumer(b *testing.B, factory taskq.Factory) {
	ctx := context.Background()

	once.Do(func() {
		q = factory.RegisterQueue(&taskq.QueueOptions{
			Name:  "bench",
			Redis: redisRing(),
		})

		task = taskq.RegisterTask(&taskq.TaskOptions{
			Name: "bench",
			Handler: func() {
				wg.Done()
			},
		})

		_ = q.Consumer().Start(ctx)
	})

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for j := 0; j < 100; j++ {
			wg.Add(1)
			_ = q.Add(ctx, task.WithArgs(ctx))
		}
		wg.Wait()
	}
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
	_ = ring.FlushDB(context.TODO()).Err()
	return ring
}
