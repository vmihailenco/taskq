package processor_test

import (
	"testing"

	"github.com/iron-io/iron_go3/mq"

	"gopkg.in/queue.v1/ironmq"
)

func TestIronmqProcessor(t *testing.T) {
	testProcessor(t, ironmq.NewQueue(mq.New("test-ironmq-processor"), &ironmq.Options{}))
}

func TestIronmqDelay(t *testing.T) {
	testDelay(t, ironmq.NewQueue(mq.New("test-ironmq-delay"), &ironmq.Options{}))
}

func TestIronmqRetry(t *testing.T) {
	testRetry(t, ironmq.NewQueue(mq.New("test-ironmq-retry"), &ironmq.Options{}))
}

func TestIronmqRateLimit(t *testing.T) {
	testRateLimit(t, ironmq.NewQueue(mq.New("test-ironmq-rate-limit"), &ironmq.Options{}))
}

func TestIronmqDelayer(t *testing.T) {
	testDelayer(t, ironmq.NewQueue(mq.New("test-ironmq-delayer"), &ironmq.Options{}))
}
