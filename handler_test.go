package queue

import (
	"bytes"
	"crypto/rand"
	"io"
	"testing"

	"gopkg.in/vmihailenco/msgpack.v2"
)

func TestEncode(t *testing.T) {
	encoded, err := encodeArgs([]interface{}{1})
	if err != nil {
		t.Fatalf("encode failed: %s", err)
	}

	decoded, err := decodeArgs(encoded)
	if err != nil {
		t.Fatalf("decode failed: %s", err)
	}

	dec := msgpack.NewDecoder(bytes.NewReader(decoded))

	n1, err := dec.DecodeInt64()
	if err != nil {
		t.Fatal(err)
	}
	if n1 != 1 {
		t.Fatalf("got %d, expected 1", n1)
	}
}

func TestEncodeZeroArgs(t *testing.T) {
	encoded, err := encodeArgs([]interface{}{})
	if err != nil {
		t.Fatalf("encode failed: %s", err)
	}

	decoded, err := decodeArgs(encoded)
	if err != nil {
		t.Fatalf("decode failed: %s", err)
	}

	dec := msgpack.NewDecoder(bytes.NewReader(decoded))
	_, err = dec.DecodeInt64()
	if err != io.EOF {
		t.Fatalf("got %v, expected EOF", err)
	}
}

func BenchmarkEncodeInts(b *testing.B) {
	for i := 0; i < b.N; i++ {
		if _, err := encodeArgs([]interface{}{1, 2, 3}); err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkEncodeBytes(b *testing.B, n int) {
	b.StopTimer()
	buf := make([]byte, n)
	rand.Read(buf)
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		if _, err := encodeArgs([]interface{}{buf}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEncodeBytes1k(b *testing.B) {
	benchmarkEncodeBytes(b, 1e3)
}

func BenchmarkEncodeBytes16k(b *testing.B) {
	benchmarkEncodeBytes(b, 16e3)
}

func BenchmarkEncodeBytes32k(b *testing.B) {
	benchmarkEncodeBytes(b, 32e3)
}

func BenchmarkEncodeBytes64k(b *testing.B) {
	benchmarkEncodeBytes(b, 64e3)
}
