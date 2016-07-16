package memqueue_test

import (
	"testing"

	"gopkg.in/queue.v1"
	"gopkg.in/queue.v1/memqueue"
)

func BenchmarkCallAsync(b *testing.B) {
	q := memqueue.NewQueue(&queue.Options{
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
	q := memqueue.NewQueue(&queue.Options{
		Storage:    memqueueStorage{redisRing()},
		Handler:    func() {},
		BufferSize: 1000000,
	})
	defer q.Close()

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			msg := queue.NewMessage()
			msg.Name = "myname"
			q.Add(msg)
		}
	})
}
