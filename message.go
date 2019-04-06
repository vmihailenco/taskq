package taskq

import (
	"bytes"
	"errors"
	"fmt"
	"hash/fnv"
	"time"

	"github.com/valyala/gozstd"
	"github.com/vmihailenco/msgpack"

	"github.com/vmihailenco/taskq/internal"
)

// ErrDuplicate is returned when adding duplicate message to the queue.
var ErrDuplicate = errors.New("taskq: message with such name already exists")

// Message is used to create and retrieve messages from a queue.
type Message struct {
	// SQS/IronMQ message id.
	ID string `msgpack:"-"`

	// Optional name for the message. Messages with the same name
	// are processed only once.
	Name string `msgpack:"-"`

	// Delay specifies the duration the queue must wait
	// before executing the message.
	Delay time.Duration `msgpack:"-"`

	// Function args passed to the handler.
	Args []interface{} `msgpack:"-"`

	// Binary representation of the args.
	ArgsCompressed bool
	ArgsBin        []byte

	// SQS/IronMQ reservation id that is used to release/delete the message.
	ReservationID string `msgpack:"-"`

	// The number of times the message has been reserved or released.
	ReservedCount int `msgpack:"-"`

	TaskName  string
	Task      *Task `msgpack:"-"`
	StickyErr error `msgpack:"-"`

	bin []byte
}

func NewMessage(args ...interface{}) *Message {
	return &Message{
		Args: args,
	}
}

func (m *Message) String() string {
	return fmt.Sprintf("Message<Id=%q Name=%q ReservedCount=%d>",
		m.ID, m.Name, m.ReservedCount)
}

// SetDelayName sets delay and generates message name from the args.
func (m *Message) SetDelayName(delay time.Duration, args ...interface{}) {
	h := hashArgs(append(args, delay, timeSlot(delay)))
	m.Name = string(h)
	m.Delay = delay + 5*time.Second
}

func (m *Message) MarshalArgs() ([]byte, error) {
	if m.ArgsBin != nil {
		return m.ArgsBin, nil
	}

	b, err := msgpack.Marshal(m.Args)
	if err != nil {
		return nil, err
	}

	m.ArgsBin = b
	return b, nil
}

func (m *Message) MarshalBinary() ([]byte, error) {
	if m.TaskName == "" {
		return nil, internal.ErrTaskNameRequired
	}
	if m.bin != nil {
		return m.bin, nil
	}

	_, err := m.MarshalArgs()
	if err != nil {
		return nil, err
	}

	if len(m.ArgsBin) > 512 {
		compressed := gozstd.Compress(nil, m.ArgsBin)
		if len(compressed) < len(m.ArgsBin) {
			m.ArgsCompressed = true
			m.ArgsBin = compressed
		}
	}

	b, err := msgpack.Marshal(m)
	if err != nil {
		return nil, err
	}

	m.bin = b
	return b, nil
}

func (m *Message) UnmarshalBinary(b []byte) error {
	err := msgpack.Unmarshal(b, m)
	if err != nil {
		return err
	}

	if m.ArgsCompressed {
		b, err = gozstd.Decompress(nil, m.ArgsBin)
		if err != nil {
			return err
		}

		m.ArgsCompressed = false
		m.ArgsBin = b
	}

	return nil
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
