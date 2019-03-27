package taskq_test

import (
	"os"
	"testing"

	"github.com/vmihailenco/taskq"
	"github.com/vmihailenco/taskq/azsqs"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

var accountID string

func init() {
	accountID = os.Getenv("AWS_ACCOUNT_ID")
}

func awsSQS() *sqs.SQS {
	return sqs.New(session.New())
}

func azsqsManager() taskq.Manager {
	return azsqs.NewManager(awsSQS(), accountID)
}

func TestSQSConsumer(t *testing.T) {
	testConsumer(t, azsqsManager(), &taskq.QueueOptions{
		Name: queueName("consumer"),
	})
}

func TestSQSFallback(t *testing.T) {
	testFallback(t, azsqsManager(), &taskq.QueueOptions{
		Name: queueName("fallback"),
	})
}

func TestSQSDelay(t *testing.T) {
	testDelay(t, azsqsManager(), &taskq.QueueOptions{
		Name: queueName("delay"),
	})
}

func TestSQSRetry(t *testing.T) {
	testRetry(t, azsqsManager(), &taskq.QueueOptions{
		Name: queueName("retry"),
	})
}

func TestSQSNamedMessage(t *testing.T) {
	testNamedMessage(t, azsqsManager(), &taskq.QueueOptions{
		Name: queueName("named-message"),
	})
}

func TestSQSCallOnce(t *testing.T) {
	testCallOnce(t, azsqsManager(), &taskq.QueueOptions{
		Name: queueName("call-once"),
	})
}

func TestSQSLen(t *testing.T) {
	testLen(t, azsqsManager(), &taskq.QueueOptions{
		Name: queueName("queue-len"),
	})
}

func TestSQSRateLimit(t *testing.T) {
	testRateLimit(t, azsqsManager(), &taskq.QueueOptions{
		Name: queueName("rate-limit"),
	})
}

func TestSQSErrorDelay(t *testing.T) {
	testErrorDelay(t, azsqsManager(), &taskq.QueueOptions{
		Name: queueName("delayer"),
	})
}

func TestSQSWorkerLimit(t *testing.T) {
	testWorkerLimit(t, azsqsManager(), &taskq.QueueOptions{
		Name: queueName("worker-limit"),
	})
}

func TestSQSInvalidCredentials(t *testing.T) {
	man := azsqs.NewManager(awsSQS(), "123")
	testInvalidCredentials(t, man, &taskq.QueueOptions{
		Name: queueName("invalid-credentials"),
	})
}

func TestSQSBatchConsumerSmallMessage(t *testing.T) {
	testBatchConsumer(t, azsqsManager(), &taskq.QueueOptions{
		Name: queueName("batch-consumer-small-message"),
	}, 100)
}

func TestSQSBatchConsumerLarge(t *testing.T) {
	testBatchConsumer(t, azsqsManager(), &taskq.QueueOptions{
		Name: queueName("batch-processor-large-message"),
	}, 64000)
}
