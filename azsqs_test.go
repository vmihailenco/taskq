package msgqueue_test

import (
	"os"
	"testing"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"

	"github.com/go-msgqueue/msgqueue"
	"github.com/go-msgqueue/msgqueue/azsqs"
)

var accountId string

func init() {
	accountId = os.Getenv("AWS_ACCOUNT_ID")
}

func awsSQS() *sqs.SQS {
	return sqs.New(session.New())
}

func azsqsManager() msgqueue.Manager {
	return azsqs.NewManager(awsSQS(), accountId)
}

func TestSQSProcessor(t *testing.T) {
	testProcessor(t, azsqsManager(), &msgqueue.Options{
		Name: queueName("sqs-processor"),
	})
}

func TestSQSCompress(t *testing.T) {
	testProcessor(t, azsqsManager(), &msgqueue.Options{
		Name:     queueName("sqs-compress"),
		Compress: true,
	})
}

func TestSQSFallback(t *testing.T) {
	testFallback(t, azsqsManager(), &msgqueue.Options{
		Name: queueName("sqs-fallback"),
	})
}

func TestSQSDelay(t *testing.T) {
	testDelay(t, azsqsManager(), &msgqueue.Options{
		Name: queueName("sqs-delay"),
	})
}

func TestSQSRetry(t *testing.T) {
	testRetry(t, azsqsManager(), &msgqueue.Options{
		Name: queueName("sqs-retry"),
	})
}

func TestSQSNamedMessage(t *testing.T) {
	testNamedMessage(t, azsqsManager(), &msgqueue.Options{
		Name: queueName("sqs-named-message"),
	})
}

func TestSQSCallOnce(t *testing.T) {
	testCallOnce(t, azsqsManager(), &msgqueue.Options{
		Name: queueName("sqs-call-once"),
	})
}

func TestSQSLen(t *testing.T) {
	testLen(t, azsqsManager(), &msgqueue.Options{
		Name: queueName("queue-len"),
	})
}

func TestSQSRateLimit(t *testing.T) {
	testRateLimit(t, azsqsManager(), &msgqueue.Options{
		Name: queueName("sqs-rate-limit"),
	})
}

func TestSQSErrorDelay(t *testing.T) {
	testErrorDelay(t, azsqsManager(), &msgqueue.Options{
		Name: queueName("sqs-delayer"),
	})
}

func TestSQSWorkerLimit(t *testing.T) {
	testWorkerLimit(t, azsqsManager(), &msgqueue.Options{
		Name: queueName("sqs-worker-limit"),
	})
}
