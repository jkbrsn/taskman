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
	MaxExecs int       // Maximum number of executions (0 for unlimited)

	remainingRuns int // Internal counter tracking runs left (0 for unlimited/exhausted)
	index         int // Index within the heap
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
	if j.MaxExecs < 0 {
		return errors.New("job max runs cannot be negative")
	}
	// Jobs with a NextExec time more than one Cadence old would re-execute continually.
	if !j.NextExec.IsZero() && j.NextExec.Before(time.Now().Add(-j.Cadence)) {
		return errors.New("job NextExec is too early")
	}

	return nil
}

// JobOption configures a Job before it is scheduled.
type JobOption func(*Job)

// WithExecLimit limits how many times the job executes before it removes itself.
// A value of 0 means unlimited executions (default).
func WithExecLimit(maxExecs int) JobOption {
	return func(job *Job) {
		job.MaxExecs = maxExecs
	}
}

// applyJobOptions applies the given options to the job.
func applyJobOptions(job *Job, opts ...JobOption) {
	for _, opt := range opts {
		if opt != nil {
			opt(job)
		}
	}
}

// initializeExecLimit sets the remaining execs based on the max.
func (j *Job) initializeExecLimit() {
	if j.MaxExecs > 0 && j.remainingRuns == 0 {
		j.remainingRuns = j.MaxExecs
	}
	if j.MaxExecs <= 0 {
		j.remainingRuns = 0
	}
}

// consumeRun decrements the remaining execs and returns true if the job is exhausted.
// It's safe to call on jobs with no exec limit.
func (j *Job) consumeRun() bool {
	if j.MaxExecs <= 0 {
		return false
	}
	if j.remainingRuns > 0 {
		j.remainingRuns--
	}
	return j.remainingRuns <= 0
}

// inheritExecLimit preserves the remaining exec count when a job is replaced.
// This prevents the counter from resetting when a job is updated.
// If the new job has a different MaxExecs value, the limit is re-initialized.
// If the previous job's remaining execs are greater than the new MaxExecs,
// the new MaxExecs value is used.
func (j *Job) inheritExecLimit(prev *Job) {
	if prev == nil {
		j.initializeExecLimit()
		return
	}
	if j.MaxExecs <= 0 {
		j.remainingRuns = 0
		return
	}
	if prev.MaxExecs == j.MaxExecs && prev.remainingRuns > 0 && j.remainingRuns == 0 {
		if prev.remainingRuns > j.MaxExecs {
			j.remainingRuns = j.MaxExecs
			return
		}
		j.remainingRuns = prev.remainingRuns
		return
	}
	j.initializeExecLimit()
}
