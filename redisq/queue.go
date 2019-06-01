package redisq

import (
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	redlock "github.com/bsm/redis-lock"
	"github.com/go-redis/redis"
	uuid "github.com/satori/go.uuid"

	"github.com/vmihailenco/taskq"
	"github.com/vmihailenco/taskq/internal"
	"github.com/vmihailenco/taskq/internal/base"
)

const batchSize = 100

type redisStreamClient interface {
	Del(keys ...string) *redis.IntCmd

	XAdd(a *redis.XAddArgs) *redis.StringCmd
	XDel(stream string, ids ...string) *redis.IntCmd
	XLen(stream string) *redis.IntCmd
	XRangeN(stream, start, stop string, count int64) *redis.XMessageSliceCmd
	XGroupCreateMkStream(stream, group, start string) *redis.StatusCmd
	XReadGroup(a *redis.XReadGroupArgs) *redis.XStreamSliceCmd
	XAck(stream, group string, ids ...string) *redis.IntCmd
	XPendingExt(a *redis.XPendingExtArgs) *redis.XPendingExtCmd
	XTrim(key string, maxLen int64) *redis.IntCmd
	XGroupDelConsumer(stream, group, consumer string) *redis.IntCmd

	ZAdd(key string, members ...*redis.Z) *redis.IntCmd
	ZRangeByScore(key string, opt *redis.ZRangeBy) *redis.StringSliceCmd
	ZRem(key string, members ...interface{}) *redis.IntCmd
}

type Queue struct {
	base base.Queue
	opt  *taskq.QueueOptions

	consumer *taskq.Consumer

	rand  *rand.Rand
	redis redisStreamClient
	wg    sync.WaitGroup

	zset                string
	stream              string
	streamGroup         string
	streamConsumer      string
	schedulerLockPrefix string

	_closed uint32
}

var _ taskq.Queue = (*Queue)(nil)

func NewQueue(opt *taskq.QueueOptions) *Queue {
	if opt.WaitTimeout == 0 {
		opt.WaitTimeout = time.Second
	}
	opt.Init()
	if opt.Redis == nil {
		panic(fmt.Errorf("redisq: Redis client is required"))
	}
	red, ok := opt.Redis.(redisStreamClient)
	if !ok {
		panic(fmt.Errorf("redisq: Redis client must support streams"))
	}

	q := &Queue{
		opt: opt,

		rand:  rand.New(rand.NewSource(time.Now().UnixNano())),
		redis: red,

		zset:                opt.Name + ":zset",
		stream:              opt.Name + ":stream",
		streamGroup:         "taskq",
		streamConsumer:      consumer(),
		schedulerLockPrefix: opt.Name + ":scheduler-lock:",
	}

	q.wg.Add(1)
	go func() {
		defer q.wg.Done()
		q.scheduler("delayed", q.scheduleDelayed)
	}()

	q.wg.Add(1)
	go func() {
		defer q.wg.Done()
		q.scheduler("pending", q.schedulePending)
	}()

	return q
}

func consumer() string {
	s, _ := os.Hostname()
	s += "-pid" + strconv.Itoa(os.Getpid())
	return s
}

func (q *Queue) Name() string {
	return q.opt.Name
}

func (q *Queue) String() string {
	return fmt.Sprintf("Queue<Name=%s>", q.Name())
}

func (q *Queue) Options() *taskq.QueueOptions {
	return q.opt
}

func (q *Queue) HandleMessage(msg *taskq.Message) error {
	return q.base.HandleMessage(msg)
}

func (q *Queue) NewTask(opt *taskq.TaskOptions) *taskq.Task {
	return q.base.NewTask(q, opt)
}

func (q *Queue) GetTask(name string) *taskq.Task {
	return q.base.GetTask(name)
}

func (q *Queue) RemoveTask(name string) {
	q.base.RemoveTask(name)
}

func (q *Queue) Consumer() *taskq.Consumer {
	if q.consumer == nil {
		q.consumer = taskq.NewConsumer(q)
	}
	return q.consumer
}

func (q *Queue) Len() (int, error) {
	n, err := q.redis.XLen(q.stream).Result()
	return int(n), err
}

// Add adds message to the queue.
func (q *Queue) Add(msg *taskq.Message) error {
	if msg.TaskName == "" {
		return internal.ErrTaskNameRequired
	}

	if msg.ID == "" {
		msg.ID = uuid.NewV4().String()
	}

	body, err := msg.MarshalBinary()
	if err != nil {
		return err
	}

	if msg.Delay > 0 {
		tm := time.Now().Add(msg.Delay)
		return q.redis.ZAdd(q.zset, &redis.Z{
			Score:  float64(unixMs(tm)),
			Member: body,
		}).Err()
	}

	return q.redis.XAdd(&redis.XAddArgs{
		Stream: q.stream,
		Values: map[string]interface{}{
			"body": body,
		},
	}).Err()
}

func (q *Queue) ReserveN(n int, waitTimeout time.Duration) ([]taskq.Message, error) {
	streams, err := q.redis.XReadGroup(&redis.XReadGroupArgs{
		Streams:  []string{q.stream, ">"},
		Group:    q.streamGroup,
		Consumer: q.streamConsumer,
		Count:    int64(n),
		Block:    waitTimeout,
	}).Result()
	if err != nil {
		if err == redis.Nil { // timeout
			return nil, nil
		}
		if strings.HasPrefix(err.Error(), "NOGROUP") {
			q.createStreamGroup()
			return q.ReserveN(n, waitTimeout)
		}
		return nil, err
	}

	stream := &streams[0]
	msgs := make([]taskq.Message, len(stream.Messages))
	for i := range stream.Messages {
		xmsg := &stream.Messages[i]
		msg := &msgs[i]

		err = unmarshalMessage(msg, xmsg)
		if err != nil {
			msg.StickyErr = err
		}
	}

	return msgs, nil
}

func (q *Queue) createStreamGroup() {
	_ = q.redis.XGroupCreateMkStream(q.stream, q.streamGroup, "0").Err()
}

func (q *Queue) Release(msg *taskq.Message) error {
	err := q.redis.XDel(q.stream, msg.ID).Err()
	if err != nil {
		return err
	}

	msg.ReservedCount++
	return q.Add(msg)
}

// Delete deletes the message from the queue.
func (q *Queue) Delete(msg *taskq.Message) error {
	return q.redis.XAck(q.stream, q.streamGroup, msg.ID).Err()
}

// Purge deletes all messages from the queue.
func (q *Queue) Purge() error {
	_ = q.redis.Del(q.zset).Err()
	_ = q.redis.XTrim(q.stream, 0).Err()
	return nil
}

// Close is like CloseTimeout with 30 seconds timeout.
func (q *Queue) Close() error {
	return q.CloseTimeout(30 * time.Second)
}

// CloseTimeout closes the queue waiting for pending messages to be processed.
func (q *Queue) CloseTimeout(timeout time.Duration) error {
	atomic.StoreUint32(&q._closed, 1)

	_ = q.redis.XGroupDelConsumer(q.stream, q.streamGroup, q.streamConsumer).Err()

	return nil
}

func (q *Queue) closed() bool {
	return atomic.LoadUint32(&q._closed) == 1
}

func (q *Queue) scheduler(name string, fn func() (int, error)) {
	const backoff = time.Second

	lock := redlock.New(q.opt.Redis, q.schedulerLockPrefix+name, &redlock.Options{
		LockTimeout: 5 * time.Second,
	})

	for {
		if q.closed() {
			break
		}

		ok, err := lock.Lock()
		if err != nil {
			internal.Logger.Printf("redlock.Lock failed: %s", err)
			continue
		}
		if !ok {
			time.Sleep(q.schedulerBackoff())
			continue
		}

		n, err := fn()
		if err != nil {
			internal.Logger.Printf("redisq: %s failed: %s", name, err)
		}

		err = lock.Unlock()
		if err != nil {
			internal.Logger.Printf("redlock.Unlock failed: %s", err)
		}

		if err != nil || n == 0 {
			time.Sleep(q.schedulerBackoff())
		}
	}
}

func (q *Queue) schedulerBackoff() time.Duration {
	n := 250 + q.rand.Intn(250)
	return time.Duration(n) * time.Millisecond
}

func (q *Queue) scheduleDelayed() (int, error) {
	tm := time.Now()
	max := strconv.FormatInt(unixMs(tm), 10)
	bodies, err := q.redis.ZRangeByScore(q.zset, &redis.ZRangeBy{
		Min:   "-inf",
		Max:   max,
		Count: batchSize,
	}).Result()
	if err != nil {
		return 0, err
	}

	// TODO: pipeline
	for _, body := range bodies {
		err = q.redis.XAdd(&redis.XAddArgs{
			Stream: q.stream,
			Values: map[string]interface{}{
				"body": body,
			},
		}).Err()
		if err != nil {
			return 0, err
		}

		err := q.redis.ZRem(q.zset, body).Err()
		if err != nil {
			return 0, err
		}
	}

	return len(bodies), nil
}

func (q *Queue) schedulePending() (int, error) {
	tm := time.Now().Add(q.opt.ReservationTimeout)
	start := strconv.FormatInt(unixMs(tm), 10)

	pending, err := q.redis.XPendingExt(&redis.XPendingExtArgs{
		Stream: q.stream,
		Group:  q.streamGroup,
		Start:  start,
		End:    "+",
		Count:  batchSize,
	}).Result()
	if err != nil {
		if strings.HasPrefix(err.Error(), "NOGROUP") {
			q.createStreamGroup()
			return 0, nil
		}
		return 0, err
	}

	for i := range pending {
		xmsgInfo := &pending[i]
		id := xmsgInfo.ID

		xmsgs, err := q.redis.XRangeN(q.stream, id, id, 1).Result()
		if err != nil {
			return 0, err
		}
		if len(xmsgs) != 1 {
			err := fmt.Errorf("redisq: can't find peding message id=%q in stream=%q",
				id, q.stream)
			return 0, err
		}

		xmsg := &xmsgs[0]
		msg := new(taskq.Message)
		err = unmarshalMessage(msg, xmsg)
		if err != nil {
			return 0, err
		}

		err = q.Release(msg)
		if err != nil {
			return 0, err
		}
	}

	return len(pending), nil
}

func unixMs(tm time.Time) int64 {
	return tm.UnixNano() / int64(time.Millisecond)
}

func unmarshalMessage(msg *taskq.Message, xmsg *redis.XMessage) error {
	body := xmsg.Values["body"].(string)
	err := msg.UnmarshalBinary(internal.StringToBytes(body))
	if err != nil {
		return err
	}

	msg.ID = xmsg.ID
	if msg.ReservedCount == 0 {
		msg.ReservedCount = 1
	}

	return nil
}
