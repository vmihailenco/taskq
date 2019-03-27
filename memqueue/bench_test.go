package memqueue_test

import (
	"testing"

	"github.com/vmihailenco/taskq"
	"github.com/vmihailenco/taskq/memqueue"
)

func BenchmarkCallAsync(b *testing.B) {
	q := memqueue.NewQueue(&taskq.Options{
		Handler:    func() {},
		BufferSize: 1000000,
	})
	defer q.Close()

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			q.Call()
		}
	})
}

func BenchmarkNamedMessage(b *testing.B) {
	q := memqueue.NewQueue(&taskq.Options{
		Redis:      redisRing(),
		Handler:    func() {},
		BufferSize: 1000000,
	})
	defer q.Close()

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			msg := taskq.NewMessage()
			msg.Name = "myname"
			q.Add(msg)
		}
	})
}
