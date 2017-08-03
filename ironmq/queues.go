package ironmq

import (
	"fmt"
	"sync"
)

const redisQueuesKey = "queues:ironmq"

var (
	queuesMu sync.Mutex
	queues   []*Queue
)

func Queues() []*Queue {
	defer queuesMu.Unlock()
	queuesMu.Lock()
	return queues
}

func registerQueue(queue *Queue) {
	defer queuesMu.Unlock()
	queuesMu.Lock()

	if queue.Name() == "" {
		return
	}

	for _, q := range queues {
		if q.Name() == queue.Name() {
			panic(fmt.Sprintf("%s is already registered", queue))
		}
	}

	queues = append(queues, queue)
	if queue.opt.Redis != nil {
		queue.opt.Redis.SAdd(redisQueuesKey, queue.Name())
		queue.opt.Redis.Publish(redisQueuesKey, queue.Name())
	}
}
