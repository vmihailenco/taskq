package ironmq

import (
	"gopkg.in/queue.v1"
	"gopkg.in/queue.v1/processor"
)

type Options struct {
	Name    string
	Storage queue.Storager

	Processor  processor.Options
	AlwaysSync bool // if true messages are always processed synchronously
	Offline    bool // if true messages are processed locally
}

func (opt *Options) init() {}
