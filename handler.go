package taskq

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"

	"github.com/vmihailenco/msgpack"
)

var errorType = reflect.TypeOf((*error)(nil)).Elem()
var messageType = reflect.TypeOf((*Message)(nil))

// Handler is an interface for processing messages.
type Handler interface {
	HandleMessage(msg *Message) error
}

type HandlerFunc func(*Message) error

func (fn HandlerFunc) HandleMessage(msg *Message) error {
	return fn(msg)
}

type reflectFunc struct {
	fv reflect.Value // Kind() == reflect.Func
	ft reflect.Type

	returnsError bool
}

var _ Handler = (*reflectFunc)(nil)

func NewHandler(fn interface{}) Handler {
	if fn == nil {
		panic(errors.New("taskq: handler func is nil"))
	}
	if h, ok := fn.(Handler); ok {
		return h
	}

	h := reflectFunc{
		fv: reflect.ValueOf(fn),
	}
	h.ft = h.fv.Type()
	if h.ft.Kind() != reflect.Func {
		panic(fmt.Sprintf("taskq: got %s, wanted %s", h.ft.Kind(), reflect.Func))
	}

	h.returnsError = returnsError(h.ft)
	if h.returnsError && acceptsMessage(h.ft) {
		return HandlerFunc(fn.(func(*Message) error))
	}

	return &h
}

func (h *reflectFunc) HandleMessage(msg *Message) error {
	b, err := msg.MarshalArgs()
	if err != nil {
		return err
	}

	dec := msgpack.NewDecoder(bytes.NewBuffer(b))
	n, err := dec.DecodeArrayLen()
	if err != nil {
		return err
	}

	if n == -1 {
		n = 0
	}
	if n != h.ft.NumIn() {
		return fmt.Errorf("taskq: got %d args, wanted %d", n, h.ft.NumIn())
	}

	in := make([]reflect.Value, h.ft.NumIn())
	for i := 0; i < h.ft.NumIn(); i++ {
		arg := reflect.New(h.ft.In(i)).Elem()
		err = dec.DecodeValue(arg)
		if err != nil {
			err = fmt.Errorf(
				"taskq: decoding arg=%d failed (data=%.100x): %s", i, b, err)
			return err
		}
		in[i] = arg
	}

	out := h.fv.Call(in)
	if h.returnsError {
		errv := out[h.ft.NumOut()-1]
		if !errv.IsNil() {
			return errv.Interface().(error)
		}
	}

	return nil
}

func acceptsMessage(typ reflect.Type) bool {
	return typ.NumIn() == 1 && typ.In(0) == messageType
}

func returnsError(typ reflect.Type) bool {
	n := typ.NumOut()
	return n > 0 && typ.Out(n-1) == errorType
}
