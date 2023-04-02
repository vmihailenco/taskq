package memqueue_test

import (
	"context"
	"testing"

	"github.com/vmihailenco/taskq/v4"
	"github.com/vmihailenco/taskq/v4/memqueue"
)

func BenchmarkCallAsync(b *testing.B) {
	taskq.Tasks.Reset()
	ctx := context.Background()

	q := memqueue.NewQueue(&taskq.QueueOptions{
		Name:    "test",
		Storage: taskq.NewLocalStorage(),
	})
	defer q.Close()

	task := taskq.RegisterTask(&taskq.TaskOptions{
		Name:    "test",
		Handler: func() {},
	})

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = q.Add(ctx, task.WithArgs(ctx))
		}
	})
}

func BenchmarkNamedMessage(b *testing.B) {
	taskq.Tasks.Reset()
	ctx := context.Background()

	q := memqueue.NewQueue(&taskq.QueueOptions{
		Name:    "test",
		Storage: taskq.NewLocalStorage(),
	})
	defer q.Close()

	task := taskq.RegisterTask(&taskq.TaskOptions{
		Name:    "test",
		Handler: func() {},
	})

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			msg := task.WithArgs(ctx)
			msg.Name = "myname"
			q.Add(ctx, msg)
		}
	})
}
