package pgq

import (
	"context"
	"database/sql"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/uptrace/bun"
	"github.com/vmihailenco/taskq/v3"
	"github.com/vmihailenco/taskq/v3/internal"
	"github.com/vmihailenco/taskq/v3/internal/msgutil"
)

type Queue struct {
	opt *taskq.QueueOptions
	db  *bun.DB

	consumer *taskq.Consumer

	_closed uint32
}

type Message struct {
	bun.BaseModel `bun:"taskq_messages,alias:m"`

	ID            ulid.ULID `bun:"type:uuid"`
	Queue         string
	RunAt         time.Time
	ReservedCount int
	ReservedAt    time.Time
	Data          []byte
}

var _ taskq.Queue = (*Queue)(nil)

func NewQueue(db *bun.DB, opt *taskq.QueueOptions) *Queue {
	if opt.WaitTimeout == 0 {
		opt.WaitTimeout = 3 * time.Second
	}
	opt.Init()

	q := &Queue{
		opt: opt,
		db:  db,
	}

	return q
}

func (q *Queue) Name() string {
	return q.opt.Name
}

func (q *Queue) String() string {
	return fmt.Sprintf("queue=%q", q.Name())
}

func (q *Queue) Options() *taskq.QueueOptions {
	return q.opt
}

func (q *Queue) Consumer() taskq.QueueConsumer {
	if q.consumer == nil {
		q.consumer = taskq.NewConsumer(q)
	}
	return q.consumer
}

func (q *Queue) Len() (int, error) {
	return q.db.NewSelect().
		Model((*Message)(nil)).
		Where("queue = ?", q.Name()).
		Count(context.Background())
}

func (q *Queue) Add(msg *taskq.Message) error {
	return q.add(msg)
}

func (q *Queue) add(msg *taskq.Message) error {
	if msg.TaskName == "" {
		return internal.ErrTaskNameRequired
	}
	if msg.Name != "" && q.isDuplicate(msg) {
		msg.Err = taskq.ErrDuplicate
		return nil
	}

	data, err := msg.MarshalBinary()
	if err != nil {
		return err
	}

	model := &Message{
		Queue: q.Name(),
		Data:  data,
	}
	if msg.ID != "" {
		model.ID = ulid.MustParse(msg.ID)
	} else {
		model.ID = ulid.Make()
	}
	if msg.Delay > 0 {
		model.RunAt = time.Now().Add(msg.Delay)
	}

	if _, err := q.db.NewInsert().
		Model(model).
		Exec(msg.Ctx); err != nil {
		return err
	}

	msg.ID = model.ID.String()
	return nil
}

func (q *Queue) isDuplicate(msg *taskq.Message) bool {
	exists := q.opt.Storage.Exists(msg.Ctx, msgutil.FullMessageName(q, msg))
	return exists
}

func (q *Queue) ReserveN(
	ctx context.Context, n int, waitTimeout time.Duration,
) ([]taskq.Message, error) {
	if waitTimeout > 0 {
		if err := sleep(ctx, waitTimeout); err != nil {
			return nil, err
		}
	}

	subq := q.db.NewSelect().
		ColumnExpr("ctid").
		Model((*Message)(nil)).
		Where("queue = ?", q.Name()).
		Where("run_at <= ?", time.Now()).
		Where("reserved_at = ?", time.Time{}).
		OrderExpr("id ASC").
		Limit(n).
		For("UPDATE SKIP LOCKED")

	var src []Message

	if err := q.db.NewUpdate().
		Model(&src).
		With("todo", subq).
		TableExpr("todo").
		Set("reserved_count = reserved_count + 1").
		Set("reserved_at = now()").
		Where("m.ctid = todo.ctid").
		Returning("m.*").
		Scan(ctx); err != nil {
		return nil, err
	}

	msgs := make([]taskq.Message, len(src))

	for i := range msgs {
		msg := &msgs[i]
		msg.Ctx = ctx

		if err := unmarshalMessage(msg, &src[i]); err != nil {
			msg.Err = err
		}
	}

	return msgs, nil
}

func (q *Queue) Release(msg *taskq.Message) error {
	res, err := q.db.NewUpdate().
		Model((*Message)(nil)).
		Set("run_at = ?", time.Now().Add(msg.Delay)).
		Set("reserved_at = ?", time.Time{}).
		Where("queue = ?", q.Name()).
		Where("id = ?", ulid.MustParse(msg.ID)).
		Exec(msg.Ctx)
	if err != nil {
		return err
	}

	n, err := res.RowsAffected()
	if err != nil {
		return err
	}

	if n != 1 {
		return sql.ErrNoRows
	}
	return nil
}

func (q *Queue) Delete(msg *taskq.Message) error {
	res, err := q.db.NewDelete().
		Model((*Message)(nil)).
		Where("queue = ?", q.Name()).
		Where("id = ?", ulid.MustParse(msg.ID)).
		Exec(msg.Ctx)
	if err != nil {
		return err
	}

	n, err := res.RowsAffected()
	if err != nil {
		return err
	}

	if n != 1 {
		return sql.ErrNoRows
	}
	return nil
}

func (q *Queue) Purge() error {
	if _, err := q.db.NewDelete().
		Model((*Message)(nil)).
		Where("queue = ?", q.Name()).
		Exec(context.Background()); err != nil {
		return err
	}
	return nil
}

// Close is like CloseTimeout with 30 seconds timeout.
func (q *Queue) Close() error {
	return q.CloseTimeout(30 * time.Second)
}

// CloseTimeout closes the queue waiting for pending messages to be processed.
func (q *Queue) CloseTimeout(timeout time.Duration) error {
	if !atomic.CompareAndSwapUint32(&q._closed, 0, 1) {
		return nil
	}

	if q.consumer != nil {
		_ = q.consumer.StopTimeout(timeout)
	}

	return nil
}

func unmarshalMessage(dest *taskq.Message, src *Message) error {
	if err := dest.UnmarshalBinary(src.Data); err != nil {
		return err
	}

	dest.ID = src.ID.String()
	dest.ReservedCount = src.ReservedCount

	return nil
}

func sleep(ctx context.Context, d time.Duration) error {
	done := ctx.Done()
	if done == nil {
		time.Sleep(d)
		return nil
	}

	t := time.NewTimer(d)
	defer t.Stop()

	select {
	case <-t.C:
		return nil
	case <-done:
		return ctx.Err()
	}
}
