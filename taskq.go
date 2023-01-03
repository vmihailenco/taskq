package taskq

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/go-redis/redis/v8"

	"github.com/vmihailenco/taskq/v3/internal"
)

func init() {
	SetLogger(log.New(os.Stderr, "taskq: ", log.LstdFlags|log.Lshortfile))
}

// SetLogger sets the main logger of the library, when not called it uses standard error.
func SetLogger(logger Logger) {
	internal.Logger = logger
}

// Logger is a generic logging interface used in SetLogger.
type Logger interface {
	Fatal(v ...interface{})
	Fatalf(format string, v ...interface{})
	Print(v ...interface{})
	Println(v ...interface{})
	Printf(format string, v ...interface{})
}

// Factory is an interface that abstracts creation of new queues.
// It is implemented in subpackages memqueue, azsqs, and ironmq.
type Factory interface {
	RegisterQueue(*QueueOptions) Queue
	Range(func(Queue) bool)
	StartConsumers(context.Context) error
	StopConsumers() error
	Close() error
}

type Redis interface {
	Del(ctx context.Context, keys ...string) *redis.IntCmd
	SetNX(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.BoolCmd
	Pipelined(ctx context.Context, fn func(pipe redis.Pipeliner) error) ([]redis.Cmder, error)

	// Eval Required by redislock
	Eval(ctx context.Context, script string, keys []string, args ...interface{}) *redis.Cmd
	EvalSha(ctx context.Context, sha1 string, keys []string, args ...interface{}) *redis.Cmd
	ScriptExists(ctx context.Context, scripts ...string) *redis.BoolSliceCmd
	ScriptLoad(ctx context.Context, script string) *redis.StringCmd
}
