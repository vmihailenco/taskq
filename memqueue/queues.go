package memqueue

import (
	"fmt"
	"sync"
)

var (
	queuesMu sync.RWMutex
	queues   map[string]*Queue
)

func Queues() []*Queue {
	queuesMu.RLock()
	defer queuesMu.RUnlock()

	qs := make([]*Queue, 0, len(queues))
	for _, q := range queues {
		qs = append(qs, q)
	}
	return qs
}

func registerQueue(queue *Queue) {
	queuesMu.Lock()
	defer queuesMu.Unlock()

	if queue.Name() == "" {
		return
	}

	if queues == nil {
		queues = make(map[string]*Queue)
	}

	if _, ok := queues[queue.Name()]; ok {
		panic(fmt.Sprintf("%s is already registered", queue))
	}
	queues[queue.Name()] = queue
}

func unregisterQueue(queue *Queue) {
	queuesMu.Lock()
	defer queuesMu.Unlock()

	if queue.Name() == "" {
		return
	}

	delete(queues, queue.Name())
}
