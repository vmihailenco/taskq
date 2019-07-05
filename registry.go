package taskq

import (
	"fmt"
	"sync"
)

var Tasks taskRegistry

type taskRegistry struct {
	m sync.Map
}

func (r *taskRegistry) Get(name string) *Task {
	if v, ok := r.m.Load(name); ok {
		return v.(*Task)
	}
	if v, ok := r.m.Load("*"); ok {
		return v.(*Task)
	}
	return nil
}

func (r *taskRegistry) Register(task *Task) error {
	name := task.Name()
	_, loaded := r.m.LoadOrStore(name, task)
	if loaded {
		return fmt.Errorf("task=%q already exists", name)
	}
	return nil
}

func (r *taskRegistry) Unregister(task *Task) {
	r.m.Delete(task.Name())
}

func (r *taskRegistry) Reset() {
	r.m = sync.Map{}
}

func (r *taskRegistry) HandleMessage(msg *Message) error {
	task := r.Get(msg.TaskName)
	if task == nil {
		return fmt.Errorf("taskq: unknown task=%q", msg.TaskName)
	}

	opt := task.Options()
	if opt.DeferFunc != nil {
		defer opt.DeferFunc()
	}

	return task.HandleMessage(msg)
}

func (r *taskRegistry) Range(fn func(name string, task *Task) bool) {
	r.m.Range(func(key, value interface{}) bool {
		return fn(key.(string), value.(*Task))
	})
}
