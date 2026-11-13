package internal

import (
	"log"
	"os"
)

// Logger is the logger used by taskq. It can be replaced with taskq.SetLogger.
var Logger = log.New(os.Stderr, "taskq: ", log.LstdFlags|log.Lshortfile)
