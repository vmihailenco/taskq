package processor_test

import (
	"testing"

	"github.com/iron-io/iron_go3/mq"

	"gopkg.in/queue.v1"
	"gopkg.in/queue.v1/ironmq"
)

func TestIronmqProcessor(t *testing.T) {
	testProcessor(t, ironmq.NewQueue(mq.New("test-ironmq-processor"), &queue.Options{}))
}

func TestIronmqDelay(t *testing.T) {
	testDelay(t, ironmq.NewQueue(mq.New("test-ironmq-delay"), &queue.Options{}))
}

func TestIronmqRetry(t *testing.T) {
	testRetry(t, ironmq.NewQueue(mq.New("test-ironmq-retry"), &queue.Options{}))
}

func TestIronmqRateLimit(t *testing.T) {
	testRateLimit(t, ironmq.NewQueue(mq.New("test-ironmq-rate-limit"), &queue.Options{}))
}

func TestIronmqDelayer(t *testing.T) {
	testDelayer(t, ironmq.NewQueue(mq.New("test-ironmq-delayer"), &queue.Options{}))
}
