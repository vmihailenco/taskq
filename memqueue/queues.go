package memqueue

import (
	"fmt"
	"sync"
)

var (
	queuesMu sync.Mutex
	queues   = make(map[string]*Memqueue)
)

func Queues() []*Memqueue {
	defer queuesMu.Unlock()
	queuesMu.Lock()

	qs := make([]*Memqueue, 0, len(queues))
	for _, q := range queues {
		qs = append(qs, q)
	}
	return qs
}

func registerQueue(queue *Memqueue) {
	queuesMu.Lock()
	if _, ok := queues[queue.Name()]; ok {
		panic(fmt.Sprintf("%s is already registered", queue))
	}
	queues[queue.Name()] = queue
	queuesMu.Unlock()
}

func unregisterQueue(queue *Memqueue) {
	queuesMu.Lock()
	delete(queues, queue.Name())
	queuesMu.Unlock()
}
