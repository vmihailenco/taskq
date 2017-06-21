package msgqueue

type Manager interface {
	NewQueue(opt *Options) *Queue
	Queues() []*Queue
}
