package processor_test

import (
	"testing"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"

	"gopkg.in/queue.v1"
	mysqs "gopkg.in/queue.v1/sqs"
)

func awsSQS() *sqs.SQS {
	return sqs.New(session.New())
}

func TestSQSProcessor(t *testing.T) {
	testProcessor(t, mysqs.NewQueue(awsSQS(), "788427328026", &queue.Options{
		Name: "test-sqs-processor",
	}))
}

func TestSQSDelay(t *testing.T) {
	testDelay(t, mysqs.NewQueue(awsSQS(), "788427328026", &queue.Options{
		Name: "test-sqs-delay",
	}))
}

func TestSQSRetry(t *testing.T) {
	testRetry(t, mysqs.NewQueue(awsSQS(), "788427328026", &queue.Options{
		Name: "test-sqs-retry",
	}))
}

func TestSQSNamedMessage(t *testing.T) {
	testNamedMessage(t, mysqs.NewQueue(awsSQS(), "788427328026", &queue.Options{
		Name:  "test-sqs-named-message",
		Redis: redisRing(),
	}))
}

func TestSQSCallOnce(t *testing.T) {
	testCallOnce(t, mysqs.NewQueue(awsSQS(), "788427328026", &queue.Options{
		Name:  "test-sqs-call0once",
		Redis: redisRing(),
	}))
}

func TestSQSRateLimit(t *testing.T) {
	testRateLimit(t, mysqs.NewQueue(awsSQS(), "788427328026", &queue.Options{
		Name: "test-sqs-rate-limit",
	}))
}

func TestSQSDelayer(t *testing.T) {
	testDelayer(t, mysqs.NewQueue(awsSQS(), "788427328026", &queue.Options{
		Name: "test-sqs-delayer",
	}))
}
