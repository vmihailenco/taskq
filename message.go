package taskq

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/valyala/gozstd"
	"github.com/vmihailenco/msgpack/v4"

	"github.com/vmihailenco/taskq/v2/internal"
)

// ErrDuplicate is returned when adding duplicate message to the queue.
var ErrDuplicate = errors.New("taskq: message with such name already exists")

// Message is used to create and retrieve messages from a queue.
type Message struct {
	Ctx context.Context `msgpack:"-"`

	// SQS/IronMQ message id.
	ID string `msgpack:",omitempty"`

	// Optional name for the message. Messages with the same name
	// are processed only once.
	Name string `msgpack:"-"`

	// Delay specifies the duration the queue must wait
	// before executing the message.
	Delay time.Duration `msgpack:"-"`

	// Function args passed to the handler.
	Args []interface{} `msgpack:"-"`

	// Binary representation of the args.
	ArgsCompression string `msgpack:",omitempty"`
	ArgsBin         []byte

	// SQS/IronMQ reservation id that is used to release/delete the message.
	ReservationID string `msgpack:"-"`

	// The number of times the message has been reserved or released.
	ReservedCount int `msgpack:",omitempty"`

	TaskName string
	Err      error `msgpack:"-"`

	evt                *ProcessMessageEvent
	marshalBinaryCache []byte
}

func NewMessage(ctx context.Context, args ...interface{}) *Message {
	return &Message{
		Ctx:  ctx,
		Args: args,
	}
}

func (m *Message) String() string {
	return fmt.Sprintf("Message<Id=%q Name=%q ReservedCount=%d>",
		m.ID, m.Name, m.ReservedCount)
}

// OnceInPeriod uses the period and the args to generate such a message name
// that message with such args is added to the queue once in a given period.
// If args are not provided then message args are used instead.
func (m *Message) OnceInPeriod(period time.Duration, args ...interface{}) *Message {
	if len(args) == 0 {
		args = m.Args
	}

	args = append(args, period, timeSlot(period))
	b, err := msgpack.Marshal(args)
	if err != nil {
		m.Err = err
	} else {
		m.Name = internal.BytesToString(b)
	}

	m.Delay = period + 5*time.Second
	return m
}

// SetDelay sets message delay.
func (m *Message) SetDelay(delay time.Duration) *Message {
	m.Delay = delay
	return m
}

func timeSlot(period time.Duration) int64 {
	if period <= 0 {
		return 0
	}
	return time.Now().Unix() / int64(period)
}

func (m *Message) MarshalArgs() ([]byte, error) {
	if m.ArgsBin != nil {
		if m.ArgsCompression == "" {
			return m.ArgsBin, nil
		}
		if m.Args == nil {
			return gozstd.Decompress(nil, m.ArgsBin)
		}
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
	if m.marshalBinaryCache != nil {
		return m.marshalBinaryCache, nil
	}

	_, err := m.MarshalArgs()
	if err != nil {
		return nil, err
	}

	if m.ArgsCompression == "" && len(m.ArgsBin) >= 512 {
		compressed := gozstd.Compress(nil, m.ArgsBin)
		if len(compressed) < len(m.ArgsBin) {
			m.ArgsCompression = "zstd"
			m.ArgsBin = compressed
		}
	}

	b, err := msgpack.Marshal(m)
	if err != nil {
		return nil, err
	}

	m.marshalBinaryCache = b
	return b, nil
}

func (m *Message) UnmarshalBinary(b []byte) error {
	err := msgpack.Unmarshal(b, m)
	if err != nil {
		return err
	}

	switch m.ArgsCompression {
	case "":
	case "zstd":
		b, err = gozstd.Decompress(nil, m.ArgsBin)
		if err != nil {
			return err
		}
		m.ArgsCompression = ""
		m.ArgsBin = b
	default:
		return fmt.Errorf("taskq: unsupported compression=%s", m.ArgsCompression)
	}

	return nil
}
