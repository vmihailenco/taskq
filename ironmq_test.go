package msgqueue_test

import (
	"testing"

	"github.com/go-msgqueue/msgqueue"
	"github.com/go-msgqueue/msgqueue/ironmq"

	iron_config "github.com/iron-io/iron_go3/config"
)

func ironmqManager() msgqueue.Manager {
	settings := iron_config.Config("iron_mq")
	return ironmq.NewManager(&settings)
}

func TestIronmqProcessor(t *testing.T) {
	testProcessor(t, ironmqManager(), &msgqueue.Options{
		Name: queueName("ironmq-processor"),
	})
}

func TestIronmqFallback(t *testing.T) {
	testFallback(t, ironmqManager(), &msgqueue.Options{
		Name: queueName("ironmq-fallback"),
	})
}

func TestIronmqDelay(t *testing.T) {
	testDelay(t, ironmqManager(), &msgqueue.Options{
		Name: queueName("ironmq-delay"),
	})
}

func TestIronmqRetry(t *testing.T) {
	testRetry(t, ironmqManager(), &msgqueue.Options{
		Name: queueName("ironmq-retry"),
	})
}

func TestIronmqNamedMessage(t *testing.T) {
	testNamedMessage(t, ironmqManager(), &msgqueue.Options{
		Name: queueName("ironmq-named-message"),
	})
}

func TestIronmqCallOnce(t *testing.T) {
	testCallOnce(t, ironmqManager(), &msgqueue.Options{
		Name: queueName("ironmq-call-once"),
	})
}

func TestIronmqLen(t *testing.T) {
	testLen(t, ironmqManager(), &msgqueue.Options{
		Name: queueName("ironmq-len"),
	})
}

func TestIronmqRateLimit(t *testing.T) {
	testRateLimit(t, ironmqManager(), &msgqueue.Options{
		Name: queueName("ironmq-rate-limit"),
	})
}

func TestIronmqErrorDelay(t *testing.T) {
	testErrorDelay(t, ironmqManager(), &msgqueue.Options{
		Name: queueName("ironmq-delayer"),
	})
}

func TestIronmqWorkerLimit(t *testing.T) {
	testWorkerLimit(t, ironmqManager(), &msgqueue.Options{
		Name: queueName("worker-limit"),
	})
}
