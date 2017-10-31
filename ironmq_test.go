package msgqueue_test

import (
	"testing"

	"github.com/go-msgqueue/msgqueue"
	"github.com/go-msgqueue/msgqueue/ironmq"

	iron_config "github.com/iron-io/iron_go3/config"
	"github.com/iron-io/iron_go3/mq"
)

func ironmqManager() msgqueue.Manager {
	settings := iron_config.Config("iron_mq")
	return ironmq.NewManager(&settings)
}

func TestIronmqProcessor(t *testing.T) {
	testProcessor(t, ironmqManager(), &msgqueue.Options{
		Name: "ironmq-processor",
	})
}

func TestIronmqFallback(t *testing.T) {
	testFallback(t, ironmqManager(), &msgqueue.Options{
		Name: "ironmq-fallback",
	})
}

func TestIronmqDelay(t *testing.T) {
	q := ironmq.NewQueue(mq.New(queueName("ironmq-delay")), &msgqueue.Options{
		WaitTimeout: waitTimeout,
	})
	testDelay(t, q)
}

func TestIronmqRetry(t *testing.T) {
	q := ironmq.NewQueue(mq.New(queueName("ironmq-retry")), &msgqueue.Options{
		WaitTimeout: waitTimeout,
	})
	testRetry(t, q)
}

func TestIronmqNamedMessage(t *testing.T) {
	q := ironmq.NewQueue(mq.New(queueName("ironmq-named-message")), &msgqueue.Options{
		WaitTimeout: waitTimeout,
		Redis:       redisRing(),
	})
	testNamedMessage(t, q)
}

func TestIronmqCallOnce(t *testing.T) {
	q := ironmq.NewQueue(mq.New(queueName("ironmq-call-once")), &msgqueue.Options{
		WaitTimeout: waitTimeout,
		Redis:       redisRing(),
	})
	testCallOnce(t, q)
}

func TestIronmqLen(t *testing.T) {
	q := ironmq.NewQueue(mq.New(queueName("ironmq-len")), &msgqueue.Options{
		WaitTimeout: waitTimeout,
		Redis:       redisRing(),
	})
	testLen(t, q)
}

func TestIronmqRateLimit(t *testing.T) {
	q := ironmq.NewQueue(mq.New(queueName("ironmq-rate-limit")), &msgqueue.Options{
		WaitTimeout: waitTimeout,
	})
	testRateLimit(t, q)
}

func TestIronmqDelayer(t *testing.T) {
	q := ironmq.NewQueue(mq.New(queueName("ironmq-delayer")), &msgqueue.Options{
		WaitTimeout: waitTimeout,
	})
	testDelayer(t, q)
}
