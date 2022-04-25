package main

import (
	"context"
	"flag"
	"log"

	"github.com/vmihailenco/taskq/example/redisexample"
)

func main() {
	flag.Parse()

	go redisexample.LogStats()

	go func() {
		for {
			err := redisexample.MainQueue.Add(redisexample.CountTask.WithArgs(context.Background()))
			if err != nil {
				log.Fatal(err)
			}
			redisexample.IncrLocalCounter()
		}
	}()

	sig := redisexample.WaitSignal()
	log.Println(sig.String())
}
