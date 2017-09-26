package msgqueue

import "sync/atomic"

const (
	stateReady   = 0
	stateStarted = 1
	stateStopped = -1
)

type jobState struct {
	v int32
}

func (s *jobState) Start() bool {
	return atomic.CompareAndSwapInt32(&s.v, stateReady, stateStarted)
}

func (s *jobState) Stop() bool {
	for {
		if atomic.CompareAndSwapInt32(&s.v, stateStarted, stateStopped) {
			return true
		}

		if atomic.LoadInt32(&s.v) == stateStopped {
			return false
		}

		if atomic.CompareAndSwapInt32(&s.v, stateReady, stateStopped) {
			return false
		}
	}
}

func (s *jobState) Reset() bool {
	return atomic.CompareAndSwapInt32(&s.v, stateStopped, stateReady)
}

func (s *jobState) Stopped() bool {
	return atomic.LoadInt32(&s.v) != stateStarted
}
