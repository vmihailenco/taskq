package internal

import (
	"encoding/ascii85"
	"errors"
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
