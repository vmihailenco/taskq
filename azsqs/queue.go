package azsqs

import (
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/go-msgqueue/msgqueue"
	"github.com/go-msgqueue/msgqueue/internal"
	"github.com/go-msgqueue/msgqueue/internal/msgutil"
	"github.com/go-msgqueue/msgqueue/memqueue"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
)

type manager struct {
	sqs       *sqs.SQS
	accountId string
}

var _ msgqueue.Manager = (*manager)(nil)

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

	addQueue   *memqueue.Queue
	addBatcher *msgqueue.Batcher

	delQueue   *memqueue.Queue
	delBatcher *msgqueue.Batcher

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

	q.initAddQueue()
	q.initDelQueue()

	registerQueue(&q)
	return &q
}

func (q *Queue) initAddQueue() {
	opt := &msgqueue.Options{
		Name:      q.opt.Name + "-add",
		GroupName: q.opt.GroupName,

		BufferSize: 1000,
		RetryLimit: 3,
		MinBackoff: time.Second,
		Handler:    msgqueue.HandlerFunc(q.addBatcherAdd),

		Redis: q.opt.Redis,
	}
	if q.opt.Handler != nil {
		opt.FallbackHandler = msgutil.UnwrapMessageHandler(q.opt.Handler)
	}
	q.addQueue = memqueue.NewQueue(opt)
	q.addBatcher = msgqueue.NewBatcher(q.addQueue.Processor(), &msgqueue.BatcherOptions{
		Handler:  q.addBatch,
		Splitter: q.splitAddBatch,
	})
}

func (q *Queue) initDelQueue() {
	q.delQueue = memqueue.NewQueue(&msgqueue.Options{
		Name:      q.opt.Name + "-delete",
		GroupName: q.opt.GroupName,

		BufferSize: 1000,
		RetryLimit: 3,
		MinBackoff: time.Second,
		Handler:    msgqueue.HandlerFunc(q.delBatcherAdd),

		Redis: q.opt.Redis,
	})
	q.delBatcher = msgqueue.NewBatcher(q.delQueue.Processor(), &msgqueue.BatcherOptions{
		Handler:  q.deleteBatch,
		Splitter: q.splitDeleteBatch,
	})
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

func (q *Queue) AddQueue() *memqueue.Queue {
	return q.addQueue
}

func (q *Queue) DeleteQueue() *memqueue.Queue {
	return q.delQueue
}

// Add adds message to the queue.
func (q *Queue) Add(msg *msgqueue.Message) error {
	_, err := msg.EncodeArgs()
	if err != nil {
		return err
	}

	msg = msgutil.WrapMessage(msg)
	return q.addQueue.Add(msg)
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

		if *sqsMsg.Body == "_" {
			*sqsMsg.Body = ""
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

func (q *Queue) Release(msg *msgqueue.Message) error {
	in := &sqs.ChangeMessageVisibilityInput{
		QueueUrl:          aws.String(q.queueURL()),
		ReceiptHandle:     &msg.ReservationId,
		VisibilityTimeout: aws.Int64(int64(msg.Delay / time.Second)),
	}
	_, err := q.sqs.ChangeMessageVisibility(in)
	return err
}

func (q *Queue) Delete(msg *msgqueue.Message) error {
	return q.delQueue.Add(msgutil.WrapMessage(msg))
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

	if q.p != nil {
		if err := q.p.StopTimeout(timeout); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	q.addBatcher.SetSync(true)
	if err := q.addQueue.CloseTimeout(timeout); err != nil && firstErr == nil {
		firstErr = err
	}

	q.delBatcher.SetSync(true)
	if err := q.delQueue.CloseTimeout(timeout); err != nil && firstErr == nil {
		firstErr = err
	}

	return firstErr
}

func (q *Queue) Len(msg *msgqueue.Message) uint32 {
	return q.Processor().Stats().InFlight
}

func (q *Queue) addBatcherAdd(msg *msgqueue.Message) error {
	return q.addBatcher.Add(msg)
}

func (q *Queue) addBatch(msgs []*msgqueue.Message) error {
	const maxDelay = 15 * time.Minute

	if len(msgs) == 0 {
		return errors.New("azsqs: no messages to add")
	}

	in := &sqs.SendMessageBatchInput{
		QueueUrl: aws.String(q.queueURL()),
	}

	for i, msg := range msgs {
		msg, err := msgutil.UnwrapMessage(msg)
		if err != nil {
			return err
		}

		msg.Id = strconv.Itoa(i)

		body, err := msg.EncodeArgs()
		if err != nil {
			internal.Logf("EncodeArgs failed: %s", err)
			continue
		}
		if body == "" {
			body = "_" // SQS requires body.
		}

		entry := &sqs.SendMessageBatchRequestEntry{
			Id:          aws.String(msg.Id),
			MessageBody: aws.String(body),
		}
		if msg.Delay <= maxDelay {
			entry.DelaySeconds = aws.Int64(int64(msg.Delay / time.Second))
		} else {
			entry.DelaySeconds = aws.Int64(int64(maxDelay / time.Second))
			entry.MessageAttributes = map[string]*sqs.MessageAttributeValue{
				"delay": &sqs.MessageAttributeValue{
					DataType:    aws.String("String"),
					StringValue: aws.String((msg.Delay - maxDelay).String()),
				},
			}
		}

		in.Entries = append(in.Entries, entry)
	}

	out, err := q.sqs.SendMessageBatch(in)
	if err != nil {
		internal.Logf("sqs.SendMessageBatch failed: %s", err)
		return err
	}

	for _, entry := range out.Failed {
		internal.Logf(
			"sqs.SendMessageBatch failed with code=%s message=%q",
			tos(entry.Code), tos(entry.Message),
		)
		if entry.SenderFault != nil && *entry.SenderFault {
			msg := findMessageById(msgs, tos(entry.Id))
			if msg != nil {
				msg.Err = fmt.Errorf("%s: %s", tos(entry.Code), tos(entry.Message))
			}
		}
	}
	return nil
}

func (q *Queue) splitAddBatch(msgs []*msgqueue.Message) ([]*msgqueue.Message, []*msgqueue.Message) {
	const messagesLimit = 10
	const sizeLimit = 250 * 1024

	var size int
	for i, msg := range msgs {
		msg, err := msgutil.UnwrapMessage(msg)
		if err != nil {
			internal.Logf(err.Error())
			continue
		}

		body, err := msg.EncodeArgs()
		if err != nil {
			internal.Logf("EncodeArgs failed: %s", err)
			continue
		}

		size += len(body)
		if size >= sizeLimit {
			var rest []*msgqueue.Message
			return msgs[:i], append(rest, msgs[i:]...)
		}
	}

	if len(msgs) >= messagesLimit {
		return msgs, nil
	}

	return nil, msgs
}

func (q *Queue) delBatcherAdd(msg *msgqueue.Message) error {
	return q.delBatcher.Add(msg)
}

func (q *Queue) deleteBatch(msgs []*msgqueue.Message) error {
	if len(msgs) == 0 {
		return errors.New("azsqs: no messages to delete")
	}

	entries := make([]*sqs.DeleteMessageBatchRequestEntry, len(msgs))
	for i, msg := range msgs {
		msg, err := msgutil.UnwrapMessage(msg)
		if err != nil {
			return err
		}

		msg.Id = strconv.Itoa(i)
		entries[i] = &sqs.DeleteMessageBatchRequestEntry{
			Id:            aws.String(msg.Id),
			ReceiptHandle: &msg.ReservationId,
		}
	}

	in := &sqs.DeleteMessageBatchInput{
		QueueUrl: aws.String(q.queueURL()),
		Entries:  entries,
	}
	out, err := q.sqs.DeleteMessageBatch(in)
	if err != nil {
		internal.Logf("sqs.DeleteMessageBatch failed: %s", err)
		return err
	}

	for _, entry := range out.Failed {
		internal.Logf(
			"sqs.DeleteMessageBatch failed with code=%s message=%q",
			tos(entry.Code), tos(entry.Message),
		)
		if entry.SenderFault != nil && *entry.SenderFault {
			msg := findMessageById(msgs, tos(entry.Id))
			if msg != nil {
				msg.Err = fmt.Errorf("%s: %s", tos(entry.Code), tos(entry.Message))
			}
		}
	}
	return nil
}

func (q *Queue) splitDeleteBatch(msgs []*msgqueue.Message) ([]*msgqueue.Message, []*msgqueue.Message) {
	const messagesLimit = 10

	if len(msgs) >= messagesLimit {
		return msgs, nil
	}
	return nil, msgs
}

func findMessageById(msgs []*msgqueue.Message, id string) *msgqueue.Message {
	for _, msg := range msgs {
		if msg.Id == id {
			return msg
		}
	}
	return nil
}

func tos(s *string) string {
	if s == nil {
		return "<nil>"
	}
	return *s
}
