package processor_test

import (
	"testing"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"

	"gopkg.in/queue.v1/memqueue"
	memsqs "gopkg.in/queue.v1/sqs"
)

func awsSQS() *sqs.SQS {
	return sqs.New(session.New())
}

func TestSQSProcessor(t *testing.T) {
	testProcessor(t, memsqs.NewQueue(awsSQS(), "788427328026", &memqueue.Options{
		Name: "test-sqs-processor",
	}))
}

func TestSQSDelay(t *testing.T) {
	testDelay(t, memsqs.NewQueue(awsSQS(), "788427328026", &memqueue.Options{
		Name: "test-sqs-delay",
	}))
}

func TestSQSRetry(t *testing.T) {
	testRetry(t, memsqs.NewQueue(awsSQS(), "788427328026", &memqueue.Options{
		Name: "test-sqs-retry",
	}))
}

func TestSQSRateLimit(t *testing.T) {
	testRateLimit(t, memsqs.NewQueue(awsSQS(), "788427328026", &memqueue.Options{
		Name: "test-sqs-rate-limit",
	}))
}

func TestSQSDelayer(t *testing.T) {
	testDelayer(t, memsqs.NewQueue(awsSQS(), "788427328026", &memqueue.Options{
		Name: "test-sqs-delayer",
	}))
}
