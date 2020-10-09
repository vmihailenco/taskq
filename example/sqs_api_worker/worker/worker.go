package main

import (
	"context"
	"flag"
	"log"

	"github.com/vmihailenco/taskq/example/api_worker/sqs_api_worker"
)

func main() {
	flag.Parse()

	c := context.Background()

	err := sqs_api_worker.QueueFactory.StartConsumers(c)
	if err != nil {
		log.Fatal(err)
	}

	go sqs_api_worker.LogStats()

	sig := sqs_api_worker.WaitSignal()
	log.Println(sig.String())

	err = sqs_api_worker.QueueFactory.Close()
	if err != nil {
		log.Fatal(err)
	}
}
