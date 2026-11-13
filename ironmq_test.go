package taskq_test

import (
	"testing"

	iron_config "github.com/iron-io/iron_go3/config"

	"github.com/vmihailenco/taskq/v3"
	"github.com/vmihailenco/taskq/v3/ironmq"
)

func ironmqFactory() taskq.Factory {
	settings := iron_config.Config("iron_mq")
	return ironmq.NewFactory(&settings)
}

func TestIronmqConsumer(t *testing.T) {
	t.Skip()

	testConsumer(t, ironmqFactory(), &taskq.QueueOptions{
		Name: queueName("ironmq-consumer"),
	})
}

func TestIronmqUnknownTask(t *testing.T) {
	t.Skip()

	testUnknownTask(t, ironmqFactory(), &taskq.QueueOptions{
		Name: queueName("ironmq-unknown-task"),
	})
}

func TestIronmqFallback(t *testing.T) {
	t.Skip()

	testFallback(t, ironmqFactory(), &taskq.QueueOptions{
		Name: queueName("ironmq-fallback"),
	})
}

func TestIronmqDelay(t *testing.T) {
	t.Skip()

	testDelay(t, ironmqFactory(), &taskq.QueueOptions{
		Name: queueName("ironmq-delay"),
	})
}

func TestIronmqRetry(t *testing.T) {
	t.Skip()

	testRetry(t, ironmqFactory(), &taskq.QueueOptions{
		Name: queueName("ironmq-retry"),
	})
}

func TestIronmqNamedMessage(t *testing.T) {
	t.Skip()

	testNamedMessage(t, ironmqFactory(), &taskq.QueueOptions{
		Name: queueName("ironmq-named-message"),
	})
}

func TestIronmqCallOnce(t *testing.T) {
	t.Skip()

	testCallOnce(t, ironmqFactory(), &taskq.QueueOptions{
		Name: queueName("ironmq-call-once"),
	})
}

func TestIronmqLen(t *testing.T) {
	t.Skip()

	testLen(t, ironmqFactory(), &taskq.QueueOptions{
		Name: queueName("ironmq-len"),
	})
}

func TestIronmqRateLimit(t *testing.T) {
	t.Skip()

	testRateLimit(t, ironmqFactory(), &taskq.QueueOptions{
		Name: queueName("ironmq-rate-limit"),
	})
}

func TestIronmqErrorDelay(t *testing.T) {
	t.Skip()

	testErrorDelay(t, ironmqFactory(), &taskq.QueueOptions{
		Name: queueName("ironmq-delayer"),
	})
}

func TestIronmqWorkerLimit(t *testing.T) {
	t.Skip()

	testWorkerLimit(t, ironmqFactory(), &taskq.QueueOptions{
		Name: queueName("ironmq-worker-limit"),
	})
}

func TestIronmqInvalidCredentials(t *testing.T) {
	t.Skip()

	settings := &iron_config.Settings{
		ProjectId: "123",
	}
	factory := ironmq.NewFactory(settings)
	testInvalidCredentials(t, factory, &taskq.QueueOptions{
		Name: queueName("ironmq-invalid-credentials"),
	})
}
