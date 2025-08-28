package taskman

import (
	"errors"
	"time"
)

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

// Validate validates the job.
func (j *Job) Validate() error {
	// Jobs with cadence <= 0 would execute immediately and continuously.
	if j.Cadence <= 0 {
		return errors.New("invalid cadence, must be greater than 0")
	}
	// Jobs with no tasks would not do anything.
	if len(j.Tasks) == 0 {
		return errors.New("job has no tasks")
	}
	// Jobs with a NextExec time more than one Cadence old would re-execute continually.
	if j.NextExec.Before(time.Now().Add(-j.Cadence)) {
		return errors.New("job NextExec is too early")
	}

	return nil
}
