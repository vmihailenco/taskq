package sqs

import (
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"

	"gopkg.in/queue.v1"
	"gopkg.in/queue.v1/memqueue"
	"gopkg.in/queue.v1/processor"
)

type Queue struct {
	sqs       *sqs.SQS
	accountId string
	opt       *Options
	memqueue  *memqueue.Memqueue

	mu        sync.RWMutex
	_queueURL string

	sync bool
}

func NewQueue(sqs *sqs.SQS, accountId string, opt *Options) *Queue {
	q := Queue{
		sqs:       sqs,
		accountId: accountId,
		opt:       opt,
	}

	popt := opt.Processor // copy
	if !opt.Offline {
		popt.Retries = 3
		popt.Backoff = time.Second
		popt.FallbackHandler = popt.Handler
		popt.Handler = queue.HandlerFunc(q.add)
	}
	memopt := memqueue.Options{
		Name:    opt.Name,
		Storage: opt.Storage,

		Processor:   popt,
		IgnoreDelay: opt.IgnoreDelay,
	}
	q.memqueue = memqueue.NewMemqueue(&memopt)

	registerQueue(&q)
	return &q
}

func (q *Queue) Name() string {
	return q.opt.Name
}

func (q *Queue) String() string {
	return fmt.Sprintf("Queue<%s>", q.Name())
}

func (q *Queue) Processor() *processor.Processor {
	return processor.New(q, &q.opt.Processor)
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
	in := &sqs.CreateQueueInput{
		QueueName: aws.String(q.Name()),
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
	msg = queue.NewMessage(msg)
	return q.memqueue.Add(msg)
}

func (q *Queue) Call(args ...interface{}) error {
	msg := queue.NewMessage(args...)
	return q.Add(msg)
}

func (q *Queue) CallOnce(delay time.Duration, args ...interface{}) error {
	msg := queue.NewMessage(args...)
	msg.Name = fmt.Sprint(args)
	msg.Delay = delay
	return q.Add(msg)
}

func (q *Queue) AddAsync(msg *queue.Message) error {
	msg = queue.NewMessage(msg)
	return q.memqueue.AddAsync(msg)
}

func (q *Queue) CallAsync(args ...interface{}) error {
	msg := queue.NewMessage(args...)
	return q.AddAsync(msg)
}

func (q *Queue) CallOnceAsync(delay time.Duration, args ...interface{}) error {
	msg := queue.NewMessage(args...)
	msg.Name = fmt.Sprint(args)
	msg.Delay = delay
	return q.AddAsync(msg)
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
	return q.memqueue.Close()
}
