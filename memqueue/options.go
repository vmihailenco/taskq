package memqueue

import "gopkg.in/queue.v1/processor"

type Storager interface {
	Exists(key string) bool
}

type Options struct {
	Name    string
	Storage Storager

	Processor   processor.Options
	IgnoreDelay bool // if true messages are processed ignoring delays
}

func (opt *Options) init() {}
