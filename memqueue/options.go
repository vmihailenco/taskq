package memqueue

import "gopkg.in/queue.v1/processor"

type cacher interface {
	Exists(key string) bool
}

type Options struct {
	Name        string
	IgnoreDelay bool // if true message.Delay is ignored
	AlwaysSync  bool // if true messages are processed synchronously
	Cache       cacher

	Processor processor.Options
}

func (opt *Options) init() {}
