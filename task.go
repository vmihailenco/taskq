package taskq

import (
	"bytes"
	"context"
	"fmt"
	"hash/fnv"
	"time"

	"github.com/vmihailenco/msgpack"
)

var unknownTaskOpt *TaskOptions

func init() {
	SetUnknownTaskOptions(&TaskOptions{
		Name: "unknown",
	})
}

func SetUnknownTaskOptions(opt *TaskOptions) {
	opt.init()
	unknownTaskOpt = opt
}

type TaskOptions struct {
	// Task name.
	Name string

	// Function called to process a message.
	Handler interface{}
	// Function called to process failed message.
	FallbackHandler interface{}

	// Optional function used by Consumer with defer statement
	// to recover from panics.
	DeferFunc func()

	// Number of tries/releases after which the message fails permanently
	// and is deleted.
	// Default is 64 retries.
	RetryLimit int
	// Minimum backoff time between retries.
	// Default is 30 seconds.
	MinBackoff time.Duration
	// Maximum backoff time between retries.
	// Default is 30 minutes.
	MaxBackoff time.Duration

	inited bool
}

func (opt *TaskOptions) init() {
	if opt.inited {
		return
	}
	opt.inited = true

	if opt.Name == "" {
		panic("TaskOptions.Name is required")
	}
	if opt.RetryLimit == 0 {
		opt.RetryLimit = 64
	}
	if opt.MinBackoff == 0 {
		opt.MinBackoff = 30 * time.Second
	}
	if opt.MaxBackoff == 0 {
		opt.MaxBackoff = 30 * time.Minute
	}
}

type Task struct {
	opt *TaskOptions

	handler         Handler
	fallbackHandler Handler
}

func RegisterTask(opt *TaskOptions) *Task {
	opt.init()

	t := &Task{
		opt: opt,
	}

	t.handler = NewHandler(opt.Handler)
	if opt.FallbackHandler != nil {
		t.fallbackHandler = NewHandler(opt.FallbackHandler)
	}

	if err := Tasks.Register(t); err != nil {
		panic(err)
	}

	return t
}

func (t *Task) Name() string {
	return t.opt.Name
}

func (t *Task) String() string {
	return fmt.Sprintf("task=%q", t.Name())
}

func (t *Task) Options() *TaskOptions {
	return t.opt
}

func (t *Task) HandleMessage(msg *Message) error {
	if msg.StickyErr != nil {
		if t.fallbackHandler != nil {
			return t.fallbackHandler.HandleMessage(msg)
		}
		return nil
	}
	return t.handler.HandleMessage(msg)
}

func (t *Task) WithMessage(msg *Message) *Message {
	msg.TaskName = t.opt.Name
	return msg
}

func (t *Task) WithArgs(ctx context.Context, args ...interface{}) *Message {
	return t.newMessage(ctx, args...)
}

func (t *Task) newMessage(ctx context.Context, args ...interface{}) *Message {
	return t.WithMessage(NewMessage(ctx, args...))
}

func (t *Task) OnceWithArgs(ctx context.Context, period time.Duration, args ...interface{}) *Message {
	msg := t.newMessage(ctx, args...)
	msg.OnceWithArgs(period, args...)
	return msg
}

func timeSlot(period time.Duration) int64 {
	if period <= 0 {
		return 0
	}
	return time.Now().UnixNano() / int64(period)
}

func hashArgs(args []interface{}) []byte {
	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	_ = enc.EncodeMulti(args...)
	b := buf.Bytes()

	if len(b) <= 64 {
		return b
	}

	h := fnv.New128a()
	_, _ = h.Write(b)
	return h.Sum(nil)
}
