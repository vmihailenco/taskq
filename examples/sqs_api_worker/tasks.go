package sqs_api_worker

import (
	"log"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/go-redis/redis"

	"github.com/vmihailenco/taskq/v2"
	"github.com/vmihailenco/taskq/v2/azsqs"
)

var Redis = redis.NewClient(&redis.Options{
	Addr: ":6379",
})

var (
	QueueFactory = azsqs.NewFactory(sqs.New(session.New()), os.Getenv("AWS_ACCOUNT_ID"))
	MainQueue    = QueueFactory.NewQueue(&taskq.QueueOptions{
		Name:  "api-worker",
		Redis: Redis,
	})
	CountTask = taskq.NewTask(&taskq.TaskOptions{
		Name: "counter",
		Handler: func() error {
			IncrLocalCounter()
			return nil
		},
	})
)

var counter int32

func GetLocalCounter() int32 {
	return atomic.LoadInt32(&counter)
}

func IncrLocalCounter() {
	atomic.AddInt32(&counter, 1)
}

func LogStats() {
	var prev int32
	for range time.Tick(3 * time.Second) {
		n := GetLocalCounter()
		log.Printf("processed %d tasks (%d/s)", n, (n-prev)/3)
		prev = n
	}
}

func WaitSignal() os.Signal {
	ch := make(chan os.Signal, 2)
	signal.Notify(
		ch,
		syscall.SIGINT,
		syscall.SIGQUIT,
		syscall.SIGTERM,
	)
	for {
		sig := <-ch
		switch sig {
		case syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM:
			return sig
		}
	}
}
