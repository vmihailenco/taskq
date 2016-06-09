package queue

import (
	"fmt"
	"reflect"
)

var errorType = reflect.TypeOf((*error)(nil)).Elem()

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
}

var _ Handler = (*reflectFunc)(nil)

func NewHandler(fn interface{}) Handler {
	if h, ok := fn.(Handler); ok {
		return h
	}

	h := reflectFunc{
		fv: reflect.ValueOf(fn),
	}
	h.ft = h.fv.Type()
	if h.ft.Kind() != reflect.Func {
		panic(fmt.Sprintf("got %s, wanted %s", h.ft.Kind(), reflect.Func))
	}
	return &h
}

func (h *reflectFunc) HandleMessage(msg *Message) error {
	args, err := decodeArgs(msg.Body, h.ft)
	if err != nil {
		return err
	}

	out := h.fv.Call(args)
	if n := h.ft.NumOut(); n > 0 && h.ft.Out(n-1) == errorType {
		if errv := out[n-1]; !errv.IsNil() {
			return errv.Interface().(error)
		}
	}

	return nil
}
