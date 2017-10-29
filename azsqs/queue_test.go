package azsqs_test

import (
	"os"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/go-msgqueue/msgqueue"
	"github.com/go-msgqueue/msgqueue/azsqs"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

func TestQueueLen(t *testing.T) {
	s := sqs.New(session.New())
	accountId := os.Getenv("AWS_ACCOUNT_ID")
	q := azsqs.NewQueue(s, accountId, &msgqueue.Options{
		Name: queueName("queue-len"),
	})

	err := q.Purge()
	if err != nil {
		t.Fatal(err)
	}

	queueLen := 10
	for i := 0; i < queueLen; i++ {
		err := q.Call()
		if err != nil {
			t.Fatal(err)
		}
	}

	time.Sleep(time.Second)

	i, err := q.Len()
	if err != nil {
		t.Fatal(err)
	}
	if i != queueLen {
		t.Fatalf("got %d messages in queue wanted 10", i)
	}
}

func queueName(s string) string {
	version := strings.Split(runtime.Version(), " ")[0]
	version = strings.Replace(version, ".", "", -1)
	return "test-" + s + "-" + version
}
