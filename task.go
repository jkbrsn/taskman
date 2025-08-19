// Package taskman provides a simple task scheduler with a worker pool.
package taskman

import "time"

// Task is an interface for tasks that can be executed.
type Task interface {
	Execute() error
}

// simpleTask is a task that executes a function.
type simpleTask struct {
	function func() error
}

// Execute executes the function and returns the error.
func (st simpleTask) Execute() error {
	err := st.function()
	return err
}

// Job is a container for a group of tasks, with a unique ID and a cadence for scheduling.
type Job struct {
	Cadence time.Duration // Time between executions of the job
	Tasks   []Task        // Tasks in the job

	ID       string    // Unique ID for the job
	NextExec time.Time // The next time the job should be executed

	index int // Index within the heap
}
