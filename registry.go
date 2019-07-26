package taskq

import (
	"fmt"
	"sync"
)

var Tasks TaskMap

type TaskRegistry interface {
	Get(name string) *Task
}

func MessageHandler(registry TaskRegistry) func(msg *Message) error {
	return func(msg *Message) error {
		task := registry.Get(msg.TaskName)
		if task == nil {
			return fmt.Errorf("taskq: unknown task=%q", msg.TaskName)
		}

		opt := task.Options()
		if opt.DeferFunc != nil {
			defer opt.DeferFunc()
		}

		return task.HandleMessage(msg)
	}
}

type TaskMap struct {
	m sync.Map
}

func (r *TaskMap) Get(name string) *Task {
	if v, ok := r.m.Load(name); ok {
		return v.(*Task)
	}
	if v, ok := r.m.Load("*"); ok {
		return v.(*Task)
	}
	return nil
}

func (r *TaskMap) Register(opt *TaskOptions) (*Task, error) {
	opt.init()

	task := &Task{
		opt:     opt,
		handler: NewHandler(opt.Handler),
	}

	if opt.FallbackHandler != nil {
		task.fallbackHandler = NewHandler(opt.FallbackHandler)
	}

	name := task.Name()
	_, loaded := r.m.LoadOrStore(name, task)
	if loaded {
		return nil, fmt.Errorf("task=%q already exists", name)
	}
	return task, nil
}

func (r *TaskMap) Unregister(task *Task) {
	r.m.Delete(task.Name())
}

func (r *TaskMap) Reset() {
	r.m = sync.Map{}
}

func (r *TaskMap) Range(fn func(name string, task *Task) bool) {
	r.m.Range(func(key, value interface{}) bool {
		return fn(key.(string), value.(*Task))
	})
}
