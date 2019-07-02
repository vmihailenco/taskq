package main

import (
	"log"

	"github.com/vmihailenco/taskq/v2/examples/sqs_api_worker"
)

func main() {
	err := sqs_api_worker.QueueFactory.StartConsumers()
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
