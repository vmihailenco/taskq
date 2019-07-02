package taskq

import (
	"log"
	"os"

	"github.com/vmihailenco/taskq/v2/internal"
)

func init() {
	SetLogger(log.New(os.Stderr, "taskq: ", log.LstdFlags|log.Lshortfile))
}

func SetLogger(logger *log.Logger) {
	internal.Logger = logger
}
