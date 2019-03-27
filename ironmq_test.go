package taskq_test

import (
	"testing"

	"github.com/vmihailenco/taskq"
	"github.com/vmihailenco/taskq/ironmq"

	iron_config "github.com/iron-io/iron_go3/config"
)

func ironmqManager() taskq.Manager {
	settings := iron_config.Config("iron_mq")
	return ironmq.NewManager(&settings)
}

func TestIronmqConsumer(t *testing.T) {
	testConsumer(t, ironmqManager(), &taskq.QueueOptions{
		Name: queueName("ironmq-consumer"),
	})
}

func TestIronmqFallback(t *testing.T) {
	testFallback(t, ironmqManager(), &taskq.QueueOptions{
		Name: queueName("ironmq-fallback"),
	})
}

func TestIronmqDelay(t *testing.T) {
	testDelay(t, ironmqManager(), &taskq.QueueOptions{
		Name: queueName("ironmq-delay"),
	})
}

func TestIronmqRetry(t *testing.T) {
	testRetry(t, ironmqManager(), &taskq.QueueOptions{
		Name: queueName("ironmq-retry"),
	})
}

func TestIronmqNamedMessage(t *testing.T) {
	testNamedMessage(t, ironmqManager(), &taskq.QueueOptions{
		Name: queueName("ironmq-named-message"),
	})
}

func TestIronmqCallOnce(t *testing.T) {
	testCallOnce(t, ironmqManager(), &taskq.QueueOptions{
		Name: queueName("ironmq-call-once"),
	})
}

func TestIronmqLen(t *testing.T) {
	testLen(t, ironmqManager(), &taskq.QueueOptions{
		Name: queueName("ironmq-len"),
	})
}

func TestIronmqRateLimit(t *testing.T) {
	testRateLimit(t, ironmqManager(), &taskq.QueueOptions{
		Name: queueName("ironmq-rate-limit"),
	})
}

func TestIronmqErrorDelay(t *testing.T) {
	testErrorDelay(t, ironmqManager(), &taskq.QueueOptions{
		Name: queueName("ironmq-delayer"),
	})
}

func TestIronmqWorkerLimit(t *testing.T) {
	testWorkerLimit(t, ironmqManager(), &taskq.QueueOptions{
		Name: queueName("worker-limit"),
	})
}

func TestIronmqInvalidCredentials(t *testing.T) {
	settings := &iron_config.Settings{
		ProjectId: "123",
	}
	manager := ironmq.NewManager(settings)
	testInvalidCredentials(t, manager, &taskq.QueueOptions{
		Name: queueName("invalid-credentials"),
	})
}
