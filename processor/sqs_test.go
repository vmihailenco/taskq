package processor_test

import (
	"testing"

	"gopkg.in/queue.v1"
	"gopkg.in/queue.v1/azsqs"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

func awsSQS() *sqs.SQS {
	return sqs.New(session.New())
}

func TestSQSProcessor(t *testing.T) {
	testProcessor(t, azsqs.NewQueue(awsSQS(), "788427328026", &queue.Options{
		Name: "test-sqs-processor",
	}))
}

func TestSQSDelay(t *testing.T) {
	testDelay(t, azsqs.NewQueue(awsSQS(), "788427328026", &queue.Options{
		Name: "test-sqs-delay",
	}))
}

func TestSQSRetry(t *testing.T) {
	testRetry(t, azsqs.NewQueue(awsSQS(), "788427328026", &queue.Options{
		Name: "test-sqs-retry",
	}))
}

func TestSQSNamedMessage(t *testing.T) {
	testNamedMessage(t, azsqs.NewQueue(awsSQS(), "788427328026", &queue.Options{
		Name:  "test-sqs-named-message",
		Redis: redisRing(),
	}))
}

func TestSQSCallOnce(t *testing.T) {
	testCallOnce(t, azsqs.NewQueue(awsSQS(), "788427328026", &queue.Options{
		Name:  "test-sqs-call-once",
		Redis: redisRing(),
	}))
}

func TestSQSRateLimit(t *testing.T) {
	testRateLimit(t, azsqs.NewQueue(awsSQS(), "788427328026", &queue.Options{
		Name: "test-sqs-rate-limit",
	}))
}

func TestSQSDelayer(t *testing.T) {
	testDelayer(t, azsqs.NewQueue(awsSQS(), "788427328026", &queue.Options{
		Name: "test-sqs-delayer",
	}))
}
