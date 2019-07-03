package base

import (
	"sync"

	"github.com/vmihailenco/taskq/v2"
)

type Factory struct {
	queuesMu sync.RWMutex
	queues   []taskq.Queuer
}

func (f *Factory) Add(q taskq.Queuer) {
	f.queuesMu.Lock()
	f.queues = append(f.queues, q)
	f.queuesMu.Unlock()
}

func (f *Factory) Queues() []taskq.Queuer {
	f.queuesMu.RLock()
	defer f.queuesMu.RUnlock()
	return f.queues
}

func (f *Factory) StartConsumers() error {
	return f.forEachQueue(func(q taskq.Queuer) error {
		return q.Consumer().Start()
	})
}

func (f *Factory) StopConsumers() error {
	return f.forEachQueue(func(q taskq.Queuer) error {
		return q.Consumer().Stop()
	})
}

func (f *Factory) Close() error {
	return f.forEachQueue(func(q taskq.Queuer) error {
		return q.Close()
	})
}

func (f *Factory) forEachQueue(fn func(taskq.Queuer) error) error {
	var wg sync.WaitGroup
	errCh := make(chan error, 1)
	for _, q := range f.Queues() {
		wg.Add(1)
		go func(q taskq.Queuer) {
			defer wg.Done()
			err := fn(q)
			select {
			case errCh <- err:
			default:
			}
		}(q)
	}
	wg.Wait()
	select {
	case err := <-errCh:
		return err
	default:
		return nil
	}
}
