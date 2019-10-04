package internal

import (
	"bytes"
	"encoding/ascii85"
	"encoding/binary"
	"errors"

	"github.com/dgryski/go-farm"
	"github.com/vmihailenco/msgpack"
)

func MaxEncodedLen(n int) int {
	return ascii85.MaxEncodedLen(n)
}

func EncodeToString(src []byte) string {
	dst := make([]byte, MaxEncodedLen(len(src)))
	n := ascii85.Encode(dst, src)
	dst = dst[:n]
	return BytesToString(dst)
}

func DecodeString(src string) ([]byte, error) {
	dst := make([]byte, len(src))
	ndst, nsrc, err := ascii85.Decode(dst, StringToBytes(src), true)
	if err != nil {
		return nil, err
	}
	if nsrc != len(src) {
		return nil, errors.New("ascii85: src is not fully decoded")
	}
	return dst[:ndst], nil
}

func Hash(args ...interface{}) []byte {
	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	_ = enc.EncodeMulti(args...)
	b := buf.Bytes()

	if len(b) <= 32 {
		return b
	}

	lo, hi := farm.Fingerprint128(b)

	b = b[:16]
	binary.BigEndian.PutUint64(b[:8], lo)
	binary.BigEndian.PutUint64(b[8:], hi)

	return b
}
