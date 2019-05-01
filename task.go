package taskq

import (
	"bytes"
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
	Name string

	// Function called to process a message.
	Handler interface{}
	// Function called to process failed message.
	FallbackHandler interface{}

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
	queue Queue
	opt   *TaskOptions

	storage    Storage
	namePrefix string

	handler         Handler
	fallbackHandler Handler
}

func NewTask(queue Queue, opt *TaskOptions) *Task {
	opt.init()

	qopt := queue.Options()
	t := &Task{
		queue: queue,
		opt:   opt,

		storage:    queue.Options().Storage,
		namePrefix: fmt.Sprintf("taskq:%s:%s:", qopt.Name, opt.Name),
	}

	t.handler = NewHandler(opt.Handler)
	if opt.FallbackHandler != nil {
		t.fallbackHandler = NewHandler(opt.FallbackHandler)
	}

	return t
}

func (t *Task) String() string {
	return fmt.Sprintf("Task<Name=%s>", t.opt.Name)
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

// AddMessage adds message to the queue.
func (t *Task) AddMessage(msg *Message) error {
	if !t.isUniqueName(msg.Name) {
		return ErrDuplicate
	}

	msg.TaskName = t.opt.Name
	msg.Task = t
	return t.queue.Add(msg)
}

// Call creates a message using the args and adds it to the queue.
func (t *Task) Call(args ...interface{}) error {
	msg := NewMessage(args...)
	return t.AddMessage(msg)
}

// CallOnce works like Call, but it returns ErrDuplicate if message
// with such args was already added in a period.
func (t *Task) CallOnce(period time.Duration, args ...interface{}) error {
	msg := NewMessage(args...)
	msg.OnceWithArgs(period, args...)
	return t.AddMessage(msg)
}

func (t *Task) isUniqueName(name string) bool {
	if name == "" {
		return true
	}
	exists := t.storage.Exists(t.namePrefix + name)
	return !exists
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
