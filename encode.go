package queue

import (
	"bytes"
	"encoding/base64"
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

func decodeArgs(s string) ([]byte, error) {
	bytes := bytesPool.Get().([]byte)
	defer bytesPool.Put(bytes)

	bytes = extend(bytes, base64.StdEncoding.DecodedLen(len(s)))
	n, err := base64.StdEncoding.Decode(bytes, []byte(s))
	if err != nil {
		return nil, err
	}
	bytes = bytes[:n]

	return snappy.Decode(nil, bytes)
}
