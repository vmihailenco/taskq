package azsqs

import (
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/go-msgqueue/msgqueue"
	"github.com/go-msgqueue/msgqueue/internal/msgutil"
	"github.com/go-msgqueue/msgqueue/memqueue"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
)

type manager struct {
	sqs       *sqs.SQS
	accountId string
}

func (m *manager) NewQueue(opt *msgqueue.Options) msgqueue.Queue {
	return NewQueue(m.sqs, m.accountId, opt)
}

func (manager) Queues() []msgqueue.Queue {
	var queues []msgqueue.Queue
	for _, q := range Queues() {
		queues = append(queues, q)
	}
	return queues
}

func NewManager(sqs *sqs.SQS, accountId string) msgqueue.Manager {
	return &manager{
		sqs:       sqs,
		accountId: accountId,
	}
}

type Queue struct {
	sqs       *sqs.SQS
	accountId string
	opt       *msgqueue.Options
	memqueue  *memqueue.Queue

	mu        sync.RWMutex
	_queueURL string

	p *msgqueue.Processor
}

var _ msgqueue.Queue = (*Queue)(nil)

func NewQueue(sqs *sqs.SQS, accountId string, opt *msgqueue.Options) *Queue {
	opt.Init()
	q := Queue{
		sqs:       sqs,
		accountId: accountId,
		opt:       opt,
	}

	memopt := msgqueue.Options{
		Name:      opt.Name,
		GroupName: opt.GroupName,

		RetryLimit: 3,
		MinBackoff: time.Second,
		Handler:    msgqueue.HandlerFunc(q.add),

		Redis: opt.Redis,
	}
	if opt.Handler != nil {
		memopt.FallbackHandler = msgutil.MessageUnwrapperHandler(opt.Handler)
	}
	q.memqueue = memqueue.NewQueue(&memopt)

	registerQueue(&q)
	return &q
}

func (q *Queue) Name() string {
	return q.opt.Name
}

func (q *Queue) String() string {
	return fmt.Sprintf("Queue<Name=%s>", q.Name())
}

func (q *Queue) Options() *msgqueue.Options {
	return q.opt
}

func (q *Queue) Processor() *msgqueue.Processor {
	if q.p == nil {
		q.p = msgqueue.NewProcessor(q, q.opt)
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

func (q *Queue) add(msg *msgqueue.Message) error {
	const maxDelay = 15 * time.Minute

	msg = msg.Args[0].(*msgqueue.Message)

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

// Add adds message to the queue.
func (q *Queue) Add(msg *msgqueue.Message) error {
	return q.memqueue.Add(msgutil.WrapMessage(msg))
}

// Call creates a message using the args and adds it to the queue.
func (q *Queue) Call(args ...interface{}) error {
	msg := msgqueue.NewMessage(args...)
	return q.Add(msg)
}

// CallOnce works like Call, but it adds message with same args
// only once in a period.
func (q *Queue) CallOnce(period time.Duration, args ...interface{}) error {
	msg := msgqueue.NewMessage(args...)
	msg.SetDelayName(period, args...)
	return q.Add(msg)
}

func (q *Queue) ReserveN(n int) ([]*msgqueue.Message, error) {
	if n > 10 {
		n = 10
	}
	in := &sqs.ReceiveMessageInput{
		QueueUrl:              aws.String(q.queueURL()),
		MaxNumberOfMessages:   aws.Int64(int64(n)),
		WaitTimeSeconds:       aws.Int64(int64(q.opt.WaitTimeout / time.Second)),
		AttributeNames:        []*string{aws.String("ApproximateReceiveCount")},
		MessageAttributeNames: []*string{aws.String("delay")},
	}
	out, err := q.sqs.ReceiveMessage(in)
	if err != nil {
		return nil, err
	}

	msgs := make([]*msgqueue.Message, len(out.Messages))
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

		msgs[i] = &msgqueue.Message{
			Body:          *sqsMsg.Body,
			Delay:         delay,
			ReservationId: *sqsMsg.ReceiptHandle,
			ReservedCount: reservedCount,
		}
	}

	return msgs, nil
}

func (q *Queue) Release(msg *msgqueue.Message, delay time.Duration) error {
	in := &sqs.ChangeMessageVisibilityInput{
		QueueUrl:          aws.String(q.queueURL()),
		ReceiptHandle:     &msg.ReservationId,
		VisibilityTimeout: aws.Int64(int64(delay / time.Second)),
	}
	_, err := q.sqs.ChangeMessageVisibility(in)
	return err
}

func (q *Queue) Delete(msg *msgqueue.Message) error {
	in := &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(q.queueURL()),
		ReceiptHandle: &msg.ReservationId,
	}
	_, err := q.sqs.DeleteMessage(in)
	return err
}

func (q *Queue) DeleteBatch(msgs []*msgqueue.Message) error {
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

// Close is CloseTimeout with 30 seconds timeout.
func (q *Queue) Close() error {
	return q.CloseTimeout(30 * time.Second)
}

// Close closes the queue waiting for pending messages to be processed.
func (q *Queue) CloseTimeout(timeout time.Duration) error {
	var firstErr error
	if err := q.memqueue.CloseTimeout(timeout); err != nil && firstErr == nil {
		firstErr = err
	}
	if q.p != nil {
		if err := q.p.StopTimeout(timeout); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
