package taskq

import (
	"fmt"
	"sync"
)

var Tasks TaskMap

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

func (r *TaskMap) HandleMessage(msg *Message) error {
	task := r.Get(msg.TaskName)
	if task == nil {
		err := fmt.Errorf("taskq: unknown task=%q", msg.TaskName)
		return r.retry(msg, err, unknownTaskOpt)
	}

	opt := task.Options()
	if opt.DeferFunc != nil {
		defer opt.DeferFunc()
	}

	msgErr := task.HandleMessage(msg)
	if msgErr == nil {
		return nil
	}

	return r.retry(msg, msgErr, opt)
}

func (r *TaskMap) retry(msg *Message, msgErr error, opt *TaskOptions) error {
	if msg.ReservedCount < opt.RetryLimit {
		msg.Delay = exponentialBackoff(
			opt.MinBackoff, opt.MaxBackoff, msg.ReservedCount)
		if delayer, ok := msgErr.(Delayer); ok {
			msg.Delay = delayer.Delay()
		}
	} else {
		msg.Delay = -1 // don't retry
	}
	return msgErr
}
