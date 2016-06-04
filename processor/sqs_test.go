package processor_test

import (
	"testing"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"

	memsqs "gopkg.in/queue.v1/sqs"
)

func awsSQS() *sqs.SQS {
	return sqs.New(session.New())
}

func TestSQSProcessor(t *testing.T) {
	testProcessor(t, memsqs.NewQueue(awsSQS(), "788427328026", "test-sqs-processor", nil))
}

func TestSQSDelay(t *testing.T) {
	testDelay(t, memsqs.NewQueue(awsSQS(), "788427328026", "test-sqs-delay", nil))
}

func TestSQSRetry(t *testing.T) {
	testRetry(t, memsqs.NewQueue(awsSQS(), "788427328026", "test-sqs-retry", nil))
}

func TestSQSRateLimit(t *testing.T) {
	testRateLimit(t, memsqs.NewQueue(awsSQS(), "788427328026", "test-sqs-rate-limit", nil))
}
