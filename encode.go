package msgqueue

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"reflect"

	"github.com/vmihailenco/msgpack"
)

func encodeArgs(args []interface{}) (string, error) {
	b, err := msgpack.Marshal(args...)
	if err != nil {
		return "", err
	}

	s := base64.StdEncoding.EncodeToString(b)
	return s, nil
}

func decodeArgs(s string, fnType reflect.Type) ([]reflect.Value, error) {
	if fnType.NumIn() == 0 {
		return nil, nil
	}

	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return nil, err
	}

	buf := bytes.NewBuffer(b)
	dec := msgpack.NewDecoder(buf)

	in := make([]reflect.Value, fnType.NumIn())
	for i := 0; i < fnType.NumIn(); i++ {
		arg := reflect.New(fnType.In(i)).Elem()
		if err := dec.DecodeValue(arg); err != nil {
			err = fmt.Errorf("queue: arg=%d decoding failed: %s", i, err)
			return nil, err
		}
		in[i] = arg
	}

	return in, nil
}
