package main

import (
	"log"

	"github.com/vmihailenco/taskq/examples/api_worker"
)

func main() {
	go api_worker.LogStats()

	go func() {
		for {
			err := api_worker.CountTask.Call()
			if err != nil {
				log.Fatal(err)
			}
			api_worker.IncrLocalCounter()
		}
	}()

	sig := api_worker.WaitSignal()
	log.Println(sig.String())
}
