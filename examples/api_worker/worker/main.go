package main

import (
	"log"

	"github.com/vmihailenco/taskq/examples/api_worker"
)

func main() {
	err := api_worker.QueueFactory.StartConsumers()
	if err != nil {
		log.Fatal(err)
	}

	go api_worker.LogStats()

	sig := api_worker.WaitSignal()
	log.Println(sig.String())

	err = api_worker.QueueFactory.Close()
	if err != nil {
		log.Fatal(err)
	}
}
