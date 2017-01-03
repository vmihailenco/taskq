package azsqs

import (
	"fmt"
	"strconv"
	"sync"
	"time"

	"gopkg.in/queue.v1"
	"gopkg.in/queue.v1/internal"
	"gopkg.in/queue.v1/memqueue"
	"gopkg.in/queue.v1/processor"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
)

type Queue struct {
	sqs       *sqs.SQS
	accountId string
	opt       *queue.Options
	memqueue  *memqueue.Queue

	mu        sync.RWMutex
	_queueURL string

	p *processor.Processor
}

var _ processor.Queuer = (*Queue)(nil)

func NewQueue(sqs *sqs.SQS, accountId string, opt *queue.Options) *Queue {
	opt.Init()
	q := Queue{
		sqs:       sqs,
		accountId: accountId,
		opt:       opt,
	}

	memopt := queue.Options{
		Name: opt.Name,

		RetryLimit: 3,
		MinBackoff: time.Second,
		Handler:    queue.HandlerFunc(q.add),

		Redis: opt.Redis,
	}
	if opt.Handler != nil {
		memopt.FallbackHandler = internal.MessageUnwrapperHandler(opt.Handler)
	}
	q.memqueue = memqueue.NewQueue(&memopt)

	registerQueue(&q)
	return &q
}

func (q *Queue) Name() string {
	return q.opt.Name
}

func (q *Queue) String() string {
	return fmt.Sprintf("Queue<%s>", q.Name())
}

func (q *Queue) Options() *queue.Options {
	return q.opt
}

func (q *Queue) Processor() *processor.Processor {
	if q.p == nil {
		q.p = processor.New(q, q.opt)
	}
	return q.p
}

func (q *Queue) queueURL() string {
	q.mu.RLock()
	queueURL := q._queueURL
	q.mu.RUnlock()
	if queueURL != "" {
		return queueURL
	}

	q.mu.Lock()
	_, _ = q.createQueue()

	queueURL, err := q.getQueueURL()
	if err == nil {
		q._queueURL = queueURL
	}
	q.mu.Unlock()

	return queueURL
}

func (q *Queue) createQueue() (string, error) {
	visTimeout := strconv.Itoa(int(q.opt.ReservationTimeout / time.Second))
	in := &sqs.CreateQueueInput{
		QueueName: aws.String(q.Name()),
		Attributes: map[string]*string{
			"VisibilityTimeout": &visTimeout,
		},
	}
	out, err := q.sqs.CreateQueue(in)
	if err != nil {
		return "", err
	}
	return *out.QueueUrl, nil
}

func (q *Queue) getQueueURL() (string, error) {
	in := &sqs.GetQueueUrlInput{
		QueueName:              aws.String(q.Name()),
		QueueOwnerAWSAccountId: &q.accountId,
	}
	out, err := q.sqs.GetQueueUrl(in)
	if err != nil {
		return "", err
	}
	return *out.QueueUrl, nil
}

func (q *Queue) add(msg *queue.Message) error {
	const maxDelay = 15 * time.Minute

	msg = msg.Args[0].(*queue.Message)

	body, err := msg.MarshalArgs()
	if err != nil {
		return err
	}
	if body == "" {
		body = "_" // SQS requires body.
	}

	in := &sqs.SendMessageInput{
		QueueUrl:    aws.String(q.queueURL()),
		MessageBody: aws.String(body),
	}

	if msg.Delay <= maxDelay {
		in.DelaySeconds = aws.Int64(int64(msg.Delay / time.Second))
	} else {
		in.DelaySeconds = aws.Int64(int64(maxDelay / time.Second))
		in.MessageAttributes = map[string]*sqs.MessageAttributeValue{
			"delay": &sqs.MessageAttributeValue{
				DataType:    aws.String("String"),
				StringValue: aws.String((msg.Delay - maxDelay).String()),
			},
		}
	}

	out, err := q.sqs.SendMessage(in)
	if err != nil {
		return err
	}

	msg.Id = *out.MessageId
	return nil
}

func (q *Queue) Add(msg *queue.Message) error {
	return q.memqueue.Add(internal.WrapMessage(msg))
}

func (q *Queue) Call(args ...interface{}) error {
	msg := queue.NewMessage(args...)
	return q.Add(msg)
}

func (q *Queue) CallOnce(delay time.Duration, args ...interface{}) error {
	msg := queue.NewMessage(args...)
	msg.SetDelayName(delay, args...)
	return q.Add(msg)
}

func (q *Queue) ReserveN(n int) ([]queue.Message, error) {
	if n > 10 {
		n = 10
	}
	in := &sqs.ReceiveMessageInput{
		QueueUrl:              aws.String(q.queueURL()),
		MaxNumberOfMessages:   aws.Int64(int64(n)),
		WaitTimeSeconds:       aws.Int64(1),
		AttributeNames:        []*string{aws.String("ApproximateReceiveCount")},
		MessageAttributeNames: []*string{aws.String("delay")},
	}
	out, err := q.sqs.ReceiveMessage(in)
	if err != nil {
		return nil, err
	}

	msgs := make([]queue.Message, len(out.Messages))
	for i, sqsMsg := range out.Messages {
		var reservedCount int
		if v, ok := sqsMsg.Attributes["ApproximateReceiveCount"]; ok {
			reservedCount, _ = strconv.Atoi(*v)
		}

		var delay time.Duration
		if v, ok := sqsMsg.MessageAttributes["delay"]; ok {
			dur, err := time.ParseDuration(*v.StringValue)
			if err != nil {
				return nil, err
			}
			if reservedCount == 1 {
				delay = dur
			} else {
				reservedCount--
			}
		}

		msgs[i] = queue.Message{
			Body:          *sqsMsg.Body,
			Delay:         delay,
			ReservationId: *sqsMsg.ReceiptHandle,
			ReservedCount: reservedCount,
		}
	}

	return msgs, nil
}

func (q *Queue) Release(msg *queue.Message, delay time.Duration) error {
	in := &sqs.ChangeMessageVisibilityInput{
		QueueUrl:          aws.String(q.queueURL()),
		ReceiptHandle:     &msg.ReservationId,
		VisibilityTimeout: aws.Int64(int64(delay / time.Second)),
	}
	_, err := q.sqs.ChangeMessageVisibility(in)
	return err
}

func (q *Queue) Delete(msg *queue.Message) error {
	in := &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(q.queueURL()),
		ReceiptHandle: &msg.ReservationId,
	}
	_, err := q.sqs.DeleteMessage(in)
	return err
}

func (q *Queue) DeleteBatch(msgs []*queue.Message) error {
	entries := make([]*sqs.DeleteMessageBatchRequestEntry, len(msgs))
	for i, msg := range msgs {
		entries[i] = &sqs.DeleteMessageBatchRequestEntry{
			Id:            aws.String(strconv.Itoa(i)),
			ReceiptHandle: &msg.ReservationId,
		}
	}

	in := &sqs.DeleteMessageBatchInput{
		QueueUrl: aws.String(q.queueURL()),
		Entries:  entries,
	}
	_, err := q.sqs.DeleteMessageBatch(in)
	return err
}

func (q *Queue) Purge() error {
	in := &sqs.PurgeQueueInput{
		QueueUrl: aws.String(q.queueURL()),
	}
	_, err := q.sqs.PurgeQueue(in)
	return err
}

func (q *Queue) Close() error {
	var retErr error
	if err := q.memqueue.Close(); err != nil && retErr == nil {
		retErr = err
	}
	if q.p != nil {
		if err := q.p.Stop(); err != nil && retErr == nil {
			retErr = err
		}
	}
	return retErr
}
