package memqueue_test

import (
	"testing"

	"github.com/vmihailenco/taskq"
	"github.com/vmihailenco/taskq/memqueue"
)

func BenchmarkCallAsync(b *testing.B) {
	q := memqueue.NewQueue(&taskq.QueueOptions{})
	defer q.Close()

	task := q.NewTask(&taskq.TaskOptions{
		Name:    "test",
		Handler: func() {},
	})

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			task.Call()
		}
	})
}

func BenchmarkNamedMessage(b *testing.B) {
	q := memqueue.NewQueue(&taskq.QueueOptions{
		Redis: redisRing(),
	})
	defer q.Close()

	task := q.NewTask(&taskq.TaskOptions{
		Name:    "test",
		Handler: func() {},
	})

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			msg := taskq.NewMessage()
			msg.Name = "myname"
			task.AddMessage(msg)
		}
	})
}
