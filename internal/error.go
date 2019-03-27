package internal

import "errors"

var ErrNotSupported = errors.New("not supported")
var ErrTaskNameRequired = errors.New("taskq: Message.TaskName is required")
