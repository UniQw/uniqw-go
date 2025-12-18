package uniqw

import "errors"

// ErrDuplicateTask is returned when Enqueue is called with an ID that already exists for the queue.
var ErrDuplicateTask = errors.New("uniqw: duplicate task id")

// ErrUnknownState is returned when an invalid state is used.
var ErrUnknownState = errors.New("uniqw: unknown state")

// ErrActiveState is returned when an operation is not allowed on the active state.
var ErrActiveState = errors.New("uniqw: operation not allowed on active state")

// ErrTaskNotFound is returned when a task with the specified ID is not found.
var ErrTaskNotFound = errors.New("uniqw: task not found")
