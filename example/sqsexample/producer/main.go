package main

import (
	"context"
	"flag"
	"log"

	"github.com/vmihailenco/taskq/example/api_worker/sqsexample"
)

func main() {
	flag.Parse()

	go sqsexample.LogStats()

	go func() {
		for {
			err := sqsexample.MainQueue.Add(sqsexample.CountTask.WithArgs(context.Background()))
			if err != nil {
				log.Fatal(err)
			}
			sqsexample.IncrLocalCounter()
		}
	}()

	sig := sqsexample.WaitSignal()
	log.Println(sig.String())
}
