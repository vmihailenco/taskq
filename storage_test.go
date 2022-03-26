package taskq_test

import (
	"context"
	"testing"
	"time"

	"github.com/vmihailenco/taskq/v3"
)

func TestNewLocalStorage(t *testing.T) {
	localStorage := taskq.NewLocalStorage(3 * time.Second)
	exist := localStorage.Exists(context.Background(), "local_exist")
	if exist {
		t.Error("Exists should return false when an new item is not in the cache")
	}

	time.Sleep(800 * time.Millisecond)
	exist = localStorage.Exists(context.Background(), "local_exist")
	if !exist {
		t.Error("Exists should return true, as the item should still in the cache")
	}

	time.Sleep(2300 * time.Millisecond)
	exist = localStorage.Exists(context.Background(), "local_exist")
	if !exist {
		t.Error("Exists should return false,while the life time of the item is over")
	}
}
