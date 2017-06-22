package msgqueue

import "time"

type Stats struct {
	InFlight    uint32
	Deleting    uint32
	Processed   uint32
	Retries     uint32
	Fails       uint32
	AvgDuration time.Duration
}

type Processor interface {
	Stats() *Stats
	Add(msg *Message) error
	AddDelay(msg *Message, delay time.Duration) error
	Process(msg *Message) error
	Start() error
	startWorkers() bool
	Stop() error
	StopTimeout(timeout time.Duration) error
	ProcessAll() error
	ProcessOne() error
	Purge() error
}
