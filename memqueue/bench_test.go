package memqueue_test

import (
	"testing"

	"github.com/vmihailenco/taskq/v2"
	"github.com/vmihailenco/taskq/v2/memqueue"
)

func BenchmarkCallAsync(b *testing.B) {
	q := memqueue.NewQueue(&taskq.QueueOptions{
		Name: "test",
	})
	defer q.Close()

	task := taskq.NewTask(&taskq.TaskOptions{
		Name:    "test",
		Handler: func() {},
	})

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = q.Add(task.WithArgs())
		}
	})
}

func BenchmarkNamedMessage(b *testing.B) {
	q := memqueue.NewQueue(&taskq.QueueOptions{
		Name:  "test",
		Redis: redisRing(),
	})
	defer q.Close()

	task := taskq.NewTask(&taskq.TaskOptions{
		Name:    "test",
		Handler: func() {},
	})

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			msg := task.WithArgs()
			msg.Name = "myname"
			q.Add(msg)
		}
	})
}
