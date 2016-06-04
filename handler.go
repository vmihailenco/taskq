package queue

import (
	"bytes"
	"fmt"
	"reflect"

	"gopkg.in/vmihailenco/msgpack.v2"
)

var errorType = reflect.TypeOf((*error)(nil)).Elem()
var messageType = reflect.TypeOf((*Message)(nil))

type Handler interface {
	HandleMessage(msg *Message) error
}

type HandlerFunc func(*Message) error

func (fn HandlerFunc) HandleMessage(msg *Message) error {
	return fn(msg)
}

type reflectFunc struct {
	fv reflect.Value // Kind() == reflect.Func
}

func NewHandler(fn interface{}) Handler {
	if h, ok := fn.(Handler); ok {
		return h
	}

	h := reflectFunc{
		fv: reflect.ValueOf(fn),
	}
	ft := h.fv.Type()
	if ft.Kind() != reflect.Func {
		panic(fmt.Sprintf("got %s, wanted %s", ft.Kind(), reflect.Func))
	}
	return h
}

func (h reflectFunc) HandleMessage(msg *Message) error {
	b, err := decodeArgs(msg.Body)
	if err != nil {
		return err
	}

	buf := bytes.NewBuffer(b)
	dec := msgpack.NewDecoder(buf)

	ft := h.fv.Type()
	in := make([]reflect.Value, ft.NumIn())
	for i := 0; i < ft.NumIn(); i++ {
		arg := reflect.New(ft.In(i)).Elem()
		if err := dec.DecodeValue(arg); err != nil {
			return err
		}
		in[i] = arg
	}

	out := h.fv.Call(in)

	if n := ft.NumOut(); n > 0 && ft.Out(n-1) == errorType {
		if errv := out[n-1]; !errv.IsNil() {
			return errv.Interface().(error)
		}
	}

	return nil
}
