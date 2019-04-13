package ironmq

import (
	"fmt"
	"sync"
)

var (
	queuesMu sync.RWMutex
	queues   []*Queue
)

func Queues() []*Queue {
	queuesMu.RLock()
	defer queuesMu.RUnlock()

	return queues
}

func registerQueue(queue *Queue) {
	queuesMu.Lock()
	defer queuesMu.Unlock()

	if queue.Name() == "" {
		return
	}

	for _, q := range queues {
		if q.Name() == queue.Name() {
			panic(fmt.Sprintf("%s is already registered", queue))
		}
	}

	queues = append(queues, queue)
}
