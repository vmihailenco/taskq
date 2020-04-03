package taskq

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"

	"github.com/vmihailenco/msgpack/v5"
)

var contextType = reflect.TypeOf((*context.Context)(nil)).Elem()
var messageType = reflect.TypeOf((*Message)(nil))
var errorType = reflect.TypeOf((*error)(nil)).Elem()

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

	acceptsContext bool
	returnsError   bool
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
	if acceptsMessage(h.ft) {
		if h.returnsError {
			return HandlerFunc(fn.(func(*Message) error))
		}
		if h.ft.NumOut() == 0 {
			theFn := fn.(func(*Message))
			return HandlerFunc(func(msg *Message) error {
				theFn(msg)
				return nil
			})
		}
	}

	h.acceptsContext = acceptsContext(h.ft)
	return &h
}

func (h *reflectFunc) HandleMessage(msg *Message) error {
	in, err := h.fnArgs(msg)
	if err != nil {
		return err
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

func (h *reflectFunc) fnArgs(msg *Message) ([]reflect.Value, error) {
	in := make([]reflect.Value, h.ft.NumIn())
	inSaved := in

	var inStart int
	if h.acceptsContext {
		inStart = 1
		in[0] = reflect.ValueOf(msg.Ctx)
		in = in[1:]
	}

	if len(msg.Args) == len(in) {
		var hasWrongType bool
		for i, arg := range msg.Args {
			v := reflect.ValueOf(arg)
			inType := h.ft.In(inStart + i)

			if inType.Kind() == reflect.Interface {
				if !v.Type().Implements(inType) {
					hasWrongType = true
					break
				}
			} else if v.Type() != inType {
				hasWrongType = true
				break
			}

			in[i] = v
		}
		if !hasWrongType {
			return inSaved, nil
		}
	}

	b, err := msg.MarshalArgs()
	if err != nil {
		return nil, err
	}

	dec := msgpack.NewDecoder(bytes.NewBuffer(b))
	n, err := dec.DecodeArrayLen()
	if err != nil {
		return nil, err
	}

	if n == -1 {
		n = 0
	}
	if n != len(in) {
		return nil, fmt.Errorf("taskq: got %d args, wanted %d", n, len(in))
	}

	for i := 0; i < len(in); i++ {
		arg := reflect.New(h.ft.In(inStart + i)).Elem()
		err = dec.DecodeValue(arg)
		if err != nil {
			err = fmt.Errorf(
				"taskq: decoding arg=%d failed (data=%.100x): %s", i, b, err)
			return nil, err
		}
		in[i] = arg
	}

	return inSaved, nil
}

func acceptsMessage(typ reflect.Type) bool {
	return typ.NumIn() == 1 && typ.In(0) == messageType
}

func acceptsContext(typ reflect.Type) bool {
	return typ.NumIn() > 0 && typ.In(0).Implements(contextType)
}

func returnsError(typ reflect.Type) bool {
	n := typ.NumOut()
	return n > 0 && typ.Out(n-1) == errorType
}
