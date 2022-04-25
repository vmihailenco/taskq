package main

import (
	"context"
	"flag"
	"log"

	"github.com/vmihailenco/taskq/example/sqsexample"
)

func main() {
	flag.Parse()

	c := context.Background()

	err := sqsexample.QueueFactory.StartConsumers(c)
	if err != nil {
		log.Fatal(err)
	}

	go sqsexample.LogStats()

	sig := sqsexample.WaitSignal()
	log.Println(sig.String())

	err = sqsexample.QueueFactory.Close()
	if err != nil {
		log.Fatal(err)
	}
}
