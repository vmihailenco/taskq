package memqueue

import (
	"fmt"
	"sync"

	"github.com/go-msgqueue/msgqueue"
)

var (
	queuesMu sync.Mutex
	queues   = make(map[string]*Queue)
)

func Queues() []msgqueue.Queue {
	defer queuesMu.Unlock()
	queuesMu.Lock()

	qs := make([]msgqueue.Queue, 0, len(queues))
	for _, q := range queues {
		qs = append(qs, q)
	}
	return qs
}

func registerQueue(queue *Queue) {
	queuesMu.Lock()
	if _, ok := queues[queue.Name()]; ok {
		panic(fmt.Sprintf("%s is already registered", queue))
	}
	queues[queue.Name()] = queue
	queuesMu.Unlock()
}

func unregisterQueue(queue *Queue) {
	queuesMu.Lock()
	delete(queues, queue.Name())
	queuesMu.Unlock()
}
