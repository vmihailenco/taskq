package internal_test

import (
	"testing"

	"github.com/go-msgqueue/msgqueue/internal"
)

var args = []interface{}{"hello", "world", 123}
var sink string

func BenchmarkEncodeArgsNoCompress(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			s, err := internal.EncodeArgs(args, false)
			if err != nil {
				b.Fatal(err)
			}
			sink = s
		}
	})
}

func BenchmarkEncodeArgsCompress(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			s, err := internal.EncodeArgs(args, true)
			if err != nil {
				b.Fatal(err)
			}
			sink = s
		}
	})
}
