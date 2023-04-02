package taskq_test

import (
	"os"
	"testing"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"

	"github.com/vmihailenco/taskq/v3"
	"github.com/vmihailenco/taskq/v3/azsqs"
)

var accountID string

func init() {
	accountID = os.Getenv("AWS_ACCOUNT_ID")
}

func awsSQS() *sqs.SQS {
	return sqs.New(session.New())
}

func azsqsFactory() taskq.Factory {
	return azsqs.NewFactory(awsSQS(), accountID)
}

func TestSQSConsumer(t *testing.T) {
	t.Skip()

	testConsumer(t, azsqsFactory(), &taskq.QueueOptions{
		Name: queueName("sqs-consumer"),
	})
}

func TestSQSUnknownTask(t *testing.T) {
	t.Skip()

	testUnknownTask(t, azsqsFactory(), &taskq.QueueOptions{
		Name: queueName("sqs-unknown-task"),
	})
}

func TestSQSFallback(t *testing.T) {
	t.Skip()

	testFallback(t, azsqsFactory(), &taskq.QueueOptions{
		Name: queueName("sqs-fallback"),
	})
}

func TestSQSDelay(t *testing.T) {
	t.Skip()

	testDelay(t, azsqsFactory(), &taskq.QueueOptions{
		Name: queueName("sqs-delay"),
	})
}

func TestSQSRetry(t *testing.T) {
	t.Skip()

	testRetry(t, azsqsFactory(), &taskq.QueueOptions{
		Name: queueName("sqs-retry"),
	})
}

func TestSQSNamedMessage(t *testing.T) {
	t.Skip()

	testNamedMessage(t, azsqsFactory(), &taskq.QueueOptions{
		Name: queueName("sqs-named-message"),
	})
}

func TestSQSCallOnce(t *testing.T) {
	t.Skip()

	testCallOnce(t, azsqsFactory(), &taskq.QueueOptions{
		Name: queueName("sqs-call-once"),
	})
}

func TestSQSLen(t *testing.T) {
	t.Skip()

	testLen(t, azsqsFactory(), &taskq.QueueOptions{
		Name: queueName("sqs-queue-len"),
	})
}

func TestSQSRateLimit(t *testing.T) {
	t.Skip()

	testRateLimit(t, azsqsFactory(), &taskq.QueueOptions{
		Name: queueName("sqs-rate-limit"),
	})
}

func TestSQSErrorDelay(t *testing.T) {
	t.Skip()

	testErrorDelay(t, azsqsFactory(), &taskq.QueueOptions{
		Name: queueName("sqs-delayer"),
	})
}

func TestSQSWorkerLimit(t *testing.T) {
	t.Skip()

	testWorkerLimit(t, azsqsFactory(), &taskq.QueueOptions{
		Name: queueName("sqs-worker-limit"),
	})
}

func TestSQSInvalidCredentials(t *testing.T) {
	t.Skip()

	man := azsqs.NewFactory(awsSQS(), "123")
	testInvalidCredentials(t, man, &taskq.QueueOptions{
		Name: queueName("sqs-invalid-credentials"),
	})
}

func TestSQSBatchConsumerSmallMessage(t *testing.T) {
	t.Skip()

	testBatchConsumer(t, azsqsFactory(), &taskq.QueueOptions{
		Name: queueName("sqs-batch-consumer-small-message"),
	}, 100)
}

func TestSQSBatchConsumerLarge(t *testing.T) {
	t.Skip()

	testBatchConsumer(t, azsqsFactory(), &taskq.QueueOptions{
		Name: queueName("sqs-batch-processor-large-message"),
	}, 64000)
}
