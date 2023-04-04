package taskq

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/vmihailenco/taskq/v4/backend"
)

func init() {
	SetLogger(log.New(os.Stderr, "taskq: ", log.LstdFlags|log.Lshortfile))
}

func SetLogger(logger *log.Logger) {
	backend.Logger = logger
}

// Factory is an interface that abstracts creation of new queues.
// It is implemented in subpackages memqueue, azsqs, and ironmq.
type Factory interface {
	RegisterQueue(*QueueConfig) Queue
	Range(func(Queue) bool)
	StartConsumers(context.Context) error
	StopConsumers() error
	Close() error
}

type Redis interface {
	redis.Scripter

	Del(ctx context.Context, keys ...string) *redis.IntCmd
	SetNX(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.BoolCmd
	Pipelined(ctx context.Context, fn func(pipe redis.Pipeliner) error) ([]redis.Cmder, error)
}
