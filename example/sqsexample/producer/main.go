package main

import (
	"context"
	"flag"
	"log"

	"github.com/vmihailenco/taskq/example/sqsexample"
)

func main() {
	flag.Parse()

	ctx := context.Background()
	go sqsexample.LogStats()

	go func() {
		for {
			err := sqsexample.MainQueue.AddJob(ctx, sqsexample.CountTask.NewJob())
			if err != nil {
				log.Fatal(err)
			}
			sqsexample.IncrLocalCounter()
		}
	}()

	sig := sqsexample.WaitSignal()
	log.Println(sig.String())
}
