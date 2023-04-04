package memqueue_test

import (
	"context"
	"testing"

	"github.com/vmihailenco/taskq/memqueue/v4"
	"github.com/vmihailenco/taskq/v4"
)

func BenchmarkCallAsync(b *testing.B) {
	taskq.Tasks.Reset()
	ctx := context.Background()

	q := memqueue.NewQueue(&taskq.QueueConfig{
		Name:    "test",
		Storage: taskq.NewLocalStorage(),
	})
	defer q.Close()

	task := taskq.RegisterTask("test", &taskq.TaskConfig{
		Handler: func() {},
	})

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = q.AddJob(ctx, task.NewJob())
		}
	})
}

func BenchmarkNamedJob(b *testing.B) {
	taskq.Tasks.Reset()
	ctx := context.Background()

	q := memqueue.NewQueue(&taskq.QueueConfig{
		Name:    "test",
		Storage: taskq.NewLocalStorage(),
	})
	defer q.Close()

	task := taskq.RegisterTask("test", &taskq.TaskConfig{
		Handler: func() {},
	})

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			msg := task.NewJob()
			msg.Name = "myname"
			q.AddJob(ctx, msg)
		}
	})
}
