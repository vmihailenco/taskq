package msgqueue

import "time"

type Queue interface {
	Name() string
	String() string
	Options() *Options
	Processor() *Processor
	SetSync(sync bool)
	SetNoDelay(noDelay bool)
	Close() error
	CloseTimeout(timeout time.Duration) error
	Add(msg *Message) error
	Call(args ...interface{}) error
	CallOnce(period time.Duration, args ...interface{}) error
}
