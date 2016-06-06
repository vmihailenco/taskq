package processor_test

import (
	"testing"

	"github.com/iron-io/iron_go3/mq"

	"gopkg.in/queue.v1/ironmq"
	"gopkg.in/queue.v1/memqueue"
)

func TestIronmqProcessor(t *testing.T) {
	testProcessor(t, ironmq.NewQueue(mq.New("test-ironmq-processor"), &memqueue.Options{}))
}

func TestIronmqDelay(t *testing.T) {
	testDelay(t, ironmq.NewQueue(mq.New("test-ironmq-delay"), &memqueue.Options{}))
}

func TestIronmqRetry(t *testing.T) {
	testRetry(t, ironmq.NewQueue(mq.New("test-ironmq-retry"), &memqueue.Options{}))
}

func TestIronmqRateLimit(t *testing.T) {
	testRateLimit(t, ironmq.NewQueue(mq.New("test-ironmq-rate-limit"), &memqueue.Options{}))
}
