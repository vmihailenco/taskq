package queue

import (
	"bytes"
	"encoding/base64"
	"reflect"
	"sync"

	"github.com/golang/snappy"
	"gopkg.in/vmihailenco/msgpack.v2"
)

var bytesPool = sync.Pool{
	New: func() interface{} {
		return make([]byte, 0)
	},
}

func extend(b []byte, n int) []byte {
	if cap(b) >= n {
		return b[:n]
	}
	b = b[:cap(b)]
	return append(b, make([]byte, n-len(b))...)
}

func encodeArgs(args []interface{}) (string, error) {
	bytes1 := bytesPool.Get().([]byte)
	defer bytesPool.Put(bytes1)

	buf := bytes.NewBuffer(bytes1[:0])
	enc := msgpack.NewEncoder(buf)
	if err := enc.Encode(args...); err != nil {
		return "", err
	}

	bytes2 := bytesPool.Get().([]byte)
	defer bytesPool.Put(bytes2)

	bytes2 = extend(bytes2, snappy.MaxEncodedLen(buf.Len()))
	bytes2 = snappy.Encode(bytes2, buf.Bytes())

	bytes1 = extend(bytes1, base64.StdEncoding.EncodedLen(len(bytes2)))
	base64.StdEncoding.Encode(bytes1, bytes2)

	return string(bytes1), nil
}

func decodeArgs(s string, fnType reflect.Type) ([]reflect.Value, error) {
	if fnType.NumIn() == 0 {
		return nil, nil
	}

	bytes1 := bytesPool.Get().([]byte)
	defer bytesPool.Put(bytes1)

	bytes1 = extend(bytes1, base64.StdEncoding.DecodedLen(len(s)))
	n, err := base64.StdEncoding.Decode(bytes1, []byte(s))
	if err != nil {
		return nil, err
	}
	bytes1 = bytes1[:n]

	bytes2 := bytesPool.Get().([]byte)
	defer bytesPool.Put(bytes2)

	bytes2, err = snappy.Decode(bytes2, bytes1)
	if err != nil {
		return nil, err
	}

	buf := bytes.NewBuffer(bytes2)
	dec := msgpack.NewDecoder(buf)

	in := make([]reflect.Value, fnType.NumIn())
	for i := 0; i < fnType.NumIn(); i++ {
		arg := reflect.New(fnType.In(i)).Elem()
		if err := dec.DecodeValue(arg); err != nil {
			return nil, err
		}
		in[i] = arg
	}

	return in, nil
}
