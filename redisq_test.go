package taskq_test

import (
	"testing"

	"github.com/vmihailenco/taskq/v3"
	"github.com/vmihailenco/taskq/v3/redisq"
)

func redisqFactory() taskq.Factory {
	return redisq.NewFactory()
}

func TestRedisqConsumer(t *testing.T) {
	testConsumer(t, redisqFactory(), &taskq.QueueOptions{
		Name: queueName("redisq-consumer"),
	})
}

func TestRedisqUnknownTask(t *testing.T) {
	testUnknownTask(t, redisqFactory(), &taskq.QueueOptions{
		Name: queueName("redisq-unknown-task"),
	})
}

func TestRedisqFallback(t *testing.T) {
	testFallback(t, redisqFactory(), &taskq.QueueOptions{
		Name: queueName("redisq-fallback"),
	})
}

func TestRedisqDelay(t *testing.T) {
	testDelay(t, redisqFactory(), &taskq.QueueOptions{
		Name: queueName("redisq-delay"),
	})
}

func TestRedisqRetry(t *testing.T) {
	testRetry(t, redisqFactory(), &taskq.QueueOptions{
		Name: queueName("redisq-retry"),
	})
}

func TestRedisqNamedMessage(t *testing.T) {
	testNamedMessage(t, redisqFactory(), &taskq.QueueOptions{
		Name: queueName("redisq-named-message"),
	})
}

func TestRedisqCallOnce(t *testing.T) {
	testCallOnce(t, redisqFactory(), &taskq.QueueOptions{
		Name: queueName("redisq-call-once"),
	})
}

func TestRedisqLen(t *testing.T) {
	testLen(t, redisqFactory(), &taskq.QueueOptions{
		Name: queueName("redisq-queue-len"),
	})
}

func TestRedisqRateLimit(t *testing.T) {
	testRateLimit(t, redisqFactory(), &taskq.QueueOptions{
		Name: queueName("redisq-rate-limit"),
	})
}

func TestRedisqErrorDelay(t *testing.T) {
	testErrorDelay(t, redisqFactory(), &taskq.QueueOptions{
		Name: queueName("redisq-delayer"),
	})
}

func TestRedisqWorkerLimit(t *testing.T) {
	testWorkerLimit(t, redisqFactory(), &taskq.QueueOptions{
		Name: queueName("redisq-worker-limit"),
	})
}

func TestRedisqBatchConsumerSmallMessage(t *testing.T) {
	testBatchConsumer(t, redisqFactory(), &taskq.QueueOptions{
		Name: queueName("redisq-batch-consumer-small-message"),
	}, 100)
}

func TestRedisqBatchConsumerLarge(t *testing.T) {
	testBatchConsumer(t, redisqFactory(), &taskq.QueueOptions{
		Name: queueName("redisq-batch-processor-large-message"),
	}, 64000)
}
