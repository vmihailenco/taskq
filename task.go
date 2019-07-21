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
	// There are three permitted types of signature:
	// 1. A zero-argument function
	// 2. A function whose arguments are assignable in type from those which are passed in the message
	// 3. A function which takes a single `*Message` argument
	// The handler function may also optionally take a Context as a first argument and may optionally return an error.
	// If the handler takes a Context, when it is invoked it will be passed the same Context as that which was passed to
	// `StartConsumer`. If the handler returns a non-nil error the message processing will fail and will be retried/.
	Handler interface{}
	// Function called to process failed message after the specified number of retries have all failed.
	// The FallbackHandler accepts the same types of function as the Handler.
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

func (t *Task) WithArgs(ctx context.Context, args ...interface{}) *Message {
	msg := NewMessage(ctx, args...)
	msg.TaskName = t.opt.Name
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
