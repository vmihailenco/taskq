package main

import (
	"context"
	"flag"
	"log"

	"github.com/vmihailenco/taskq/v2/examples/api_worker"
)

func main() {
	flag.Parse()

	go api_worker.LogStats()

	go func() {
		for {
			err := api_worker.MainQueue.Add(api_worker.CountTask.WithArgs(context.Background()))
			if err != nil {
				log.Fatal(err)
			}
			api_worker.IncrLocalCounter()
		}
	}()

	sig := api_worker.WaitSignal()
	log.Println(sig.String())
}
