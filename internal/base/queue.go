package base

import (
	"fmt"

	"github.com/vmihailenco/taskq/v2"
)

type Queue struct {
	tasks map[string]*taskq.Task
}

func (q *Queue) HandleMessage(msg *taskq.Message) error {
	task, err := q.task(msg.TaskName)
	if err != nil {
		return err
	}
	return task.HandleMessage(msg)
}

func (q *Queue) task(taskName string) (*taskq.Task, error) {
	task := q.GetTask(taskName)
	if task == nil {
		return nil, fmt.Errorf("taskq: task=%q does not exist", taskName)
	}
	return task, nil
}

func (q *Queue) NewTask(queue taskq.Queue, opt *taskq.TaskOptions) *taskq.Task {
	task, ok := q.tasks[opt.Name]
	if ok {
		panic(fmt.Errorf("%s is already registered", task))
	}
	task = taskq.NewTask(queue, opt)
	if q.tasks == nil {
		q.tasks = make(map[string]*taskq.Task)
	}
	q.tasks[opt.Name] = task
	return task
}

func (q *Queue) GetTask(name string) *taskq.Task {
	return q.tasks[name]
}

func (q *Queue) RemoveTask(name string) {
	delete(q.tasks, name)
}
