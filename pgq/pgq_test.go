package pgq_test

import (
	"context"
	"database/sql"
	"os"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"
	"github.com/uptrace/bun/driver/pgdriver"
	"github.com/uptrace/bun/extra/bundebug"
	"github.com/uptrace/bun/migrate"
	"github.com/vmihailenco/taskq/v4"
	"github.com/vmihailenco/taskq/v4/pgq"
	"github.com/vmihailenco/taskq/v4/taskqtest"
)

func queueName(s string) string {
	version := strings.Split(runtime.Version(), " ")[0]
	version = strings.Replace(version, ".", "", -1)
	return "test-" + s + "-" + version
}

func newFactory(t *testing.T) taskq.Factory {
	dsn := "postgres://test:test@localhost:5432/test?sslmode=disable"
	sqldb := sql.OpenDB(pgdriver.NewConnector(pgdriver.WithDSN(dsn)))
	db := bun.NewDB(sqldb, pgdialect.New())

	db.AddQueryHook(bundebug.NewQueryHook(bundebug.FromEnv("BUNDEBUG")))

	f, err := os.Open("migration.sql")
	require.NoError(t, err)

	err = migrate.Exec(context.Background(), db, f, false)
	require.NoError(t, err)

	return pgq.NewFactory(db)
}

func TestConsumer(t *testing.T) {
	taskqtest.TestConsumer(t, newFactory(t), &taskq.QueueOptions{
		Name: queueName("consumer"),
	})
}

func TestUnknownTask(t *testing.T) {
	taskqtest.TestUnknownTask(t, newFactory(t), &taskq.QueueOptions{
		Name: queueName("unknown-task"),
	})
}

func TestFallback(t *testing.T) {
	taskqtest.TestFallback(t, newFactory(t), &taskq.QueueOptions{
		Name: queueName("fallback"),
	})
}

func TestDelay(t *testing.T) {
	taskqtest.TestDelay(t, newFactory(t), &taskq.QueueOptions{
		Name: queueName("delay"),
	})
}

func TestRetry(t *testing.T) {
	taskqtest.TestRetry(t, newFactory(t), &taskq.QueueOptions{
		Name: queueName("retry"),
	})
}

func TestNamedMessage(t *testing.T) {
	taskqtest.TestNamedMessage(t, newFactory(t), &taskq.QueueOptions{
		Name: queueName("named-message"),
	})
}

func TestCallOnce(t *testing.T) {
	taskqtest.TestCallOnce(t, newFactory(t), &taskq.QueueOptions{
		Name: queueName("call-once"),
	})
}

func TestLen(t *testing.T) {
	taskqtest.TestLen(t, newFactory(t), &taskq.QueueOptions{
		Name: queueName("queue-len"),
	})
}

func TestRateLimit(t *testing.T) {
	taskqtest.TestRateLimit(t, newFactory(t), &taskq.QueueOptions{
		Name: queueName("rate-limit"),
	})
}

func TestErrorDelay(t *testing.T) {
	taskqtest.TestErrorDelay(t, newFactory(t), &taskq.QueueOptions{
		Name: queueName("delayer"),
	})
}

func TestBatchConsumerSmallMessage(t *testing.T) {
	taskqtest.TestBatchConsumer(t, newFactory(t), &taskq.QueueOptions{
		Name: queueName("batch-consumer-small-message"),
	}, 100)
}

func TestBatchConsumerLarge(t *testing.T) {
	taskqtest.TestBatchConsumer(t, newFactory(t), &taskq.QueueOptions{
		Name: queueName("batch-processor-large-message"),
	}, 64000)
}
