package internal

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"reflect"
	"sync"

	"github.com/golang/snappy"
	"github.com/vmihailenco/msgpack"
)

var buffers = &sync.Pool{
	New: func() interface{} {
		return make([]byte, 1024)
	},
}

func EncodeArgs(args []interface{}, compress bool) (string, error) {
	b := buffers.Get().([]byte)
	buf := bytes.NewBuffer(b[:0])
	enc := msgpack.NewEncoder(buf)
	err := enc.Encode(args...)
	if err != nil {
		buffers.Put(b)
		return "", err
	}
	b = buf.Bytes()

	var b2 []byte
	if compress {
		b2 = buffers.Get().([]byte)
		b2 = snappy.Encode(b2[:0], b)
	} else {
		b2 = b
		b = buffers.Get().([]byte)
	}

	b = ensureLen(b, base64.StdEncoding.EncodedLen(len(b2)))
	base64.StdEncoding.Encode(b, b2)
	buffers.Put(b2)
	s := string(b)
	buffers.Put(b)

	return s, nil
}

func ensureLen(b []byte, n int) []byte {
	if n <= cap(b) {
		b = b[:n]
		return b
	}
	b = b[:cap(b)]
	b = append(b, make([]byte, n-len(b))...)
	return b
}

func DecodeArgs(s string, fnType reflect.Type, decompress bool) ([]reflect.Value, error) {
	if fnType.NumIn() == 0 && s == "" {
		return nil, nil
	}

	b := buffers.Get().([]byte)
	b = ensureLen(b, base64.StdEncoding.DecodedLen(len(s)))
	n, err := base64.StdEncoding.Decode(b, []byte(s))
	if err != nil {
		buffers.Put(b)
		return nil, err
	}
	b = b[:n]

	if decompress {
		b2 := buffers.Get().([]byte)
		b2, err = snappy.Decode(b2, b)
		buffers.Put(b)
		if err != nil {
			buffers.Put(b2)
			return nil, err
		}
		b = b2
	}

	buf := bytes.NewBuffer(b)
	dec := msgpack.NewDecoder(buf)

	in := make([]reflect.Value, fnType.NumIn())
	for i := 0; i < fnType.NumIn(); i++ {
		arg := reflect.New(fnType.In(i)).Elem()
		err = dec.DecodeValue(arg)
		if err != nil {
			err = fmt.Errorf("msgqueue: arg=%d decoding failed: %s", i, err)
			break
		}
		in[i] = arg
	}

	buffers.Put(b)
	return in, err
}
