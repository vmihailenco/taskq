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

func TestSQSProcessor(t *testing.T) {
	testProcessor(t, azsqs.NewQueue(awsSQS(), accountId, &msgqueue.Options{
		Name: queueName("sqs-processor"),
	}))
}

func TestSQSDelay(t *testing.T) {
	testDelay(t, azsqs.NewQueue(awsSQS(), accountId, &msgqueue.Options{
		Name: queueName("sqs-delay"),
	}))
}

func TestSQSRetry(t *testing.T) {
	testRetry(t, azsqs.NewQueue(awsSQS(), accountId, &msgqueue.Options{
		Name: queueName("sqs-retry"),
	}))
}

func TestSQSNamedMessage(t *testing.T) {
	testNamedMessage(t, azsqs.NewQueue(awsSQS(), accountId, &msgqueue.Options{
		Name:  queueName("sqs-named-message"),
		Redis: redisRing(),
	}))
}

func TestSQSCallOnce(t *testing.T) {
	testCallOnce(t, azsqs.NewQueue(awsSQS(), accountId, &msgqueue.Options{
		Name:  queueName("sqs-call-once"),
		Redis: redisRing(),
	}))
}

func TestSQSRateLimit(t *testing.T) {
	testRateLimit(t, azsqs.NewQueue(awsSQS(), accountId, &msgqueue.Options{
		Name: queueName("sqs-rate-limit"),
	}))
}

func TestSQSDelayer(t *testing.T) {
	testDelayer(t, azsqs.NewQueue(awsSQS(), accountId, &msgqueue.Options{
		Name: queueName("sqs-delayer"),
	}))
}
