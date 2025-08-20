// Package taskman provides a simple task scheduler with a worker pool.
package taskman

import (
	"context"
	"runtime"
	"sync"
	"time"

	"github.com/rs/xid"
	"github.com/rs/zerolog"
)

const (
	maxWorkerCount       = 4096
	defaultScaleInterval = 1 * time.Minute
	defaultBufferSize    = 64
)

// ExecMode configures how tasks are executed.
// ModePool executes tasks using a shared worker pool; ModePerJob (not yet implemented)
// would execute tasks per job.
const (
	ModePool ExecMode = iota
	ModePerJob
)

// ExecMode is the execution mode for TaskManager.
type ExecMode int

// TMOption is a functional option for the TaskManager struct.
type TMOption func(*TaskManager)

// TaskManager manages task scheduling and execution. Tasks are scheduled within Jobs, and the
// manager dispatches scheduled jobs to a worker pool for execution.
// TODO: remove mutex lock
type TaskManager struct {
	sync.RWMutex

	log    zerolog.Logger
	ctx    context.Context
	cancel context.CancelFunc

	// Shared
	metrics   *managerMetrics // Metrics for the task manager
	errorChan chan error      // Channel to receive errors from the worker pool

	// Execution
	exec     executor
	execMode ExecMode

	// Context and operations
	stopOnce sync.Once // Ensures Stop is only called once

	// Options
	channelBufferSize int           // Buffer size for task channels
	minWorkerCount    int           // Minimum number of workers in the pool
	scaleInterval     time.Duration // Interval for automatic scaling of the worker pool
}

// ErrorChannel returns a read-only channel for reading errors from task execution.
func (tm *TaskManager) ErrorChannel() <-chan error {
	return tm.errorChan
}

// Metrics returns a snapshot of the task manager's metrics.
func (tm *TaskManager) Metrics() TaskManagerMetrics {
	tm.RLock()
	defer tm.RUnlock()

	metrics := TaskManagerMetrics{
		QueuedTasks:          int(tm.metrics.tasksInQueue.Load()),
		QueueMaxJobWidth:     int(tm.metrics.maxJobWidth.Load()),
		TaskAverageExecTime:  tm.metrics.averageExecTime.Load(),
		TasksTotalExecutions: int(tm.metrics.totalTaskExecutions.Load()),
		TasksPerSecond:       tm.metrics.tasksPerSecond.Load(),
	}

	// Get pool metrics if in pool mode
	// TODO: can this be done better?
	if tm.execMode == ModePool {
		poolExecutor, ok := tm.exec.(*poolExecutor)
		if !ok {
			return metrics
		}
		// TODO: unify approach with other execution modes
		metrics.QueuedJobs = poolExecutor.jobsInQueue()
		metrics.PoolMetrics = poolExecutor.Metrics()
	}

	return metrics
}

// ScheduleFunc takes a function and adds it to the TaskManager in a Job. Creates and returns a
// randomized ID, used to identify the Job within the task manager.
func (tm *TaskManager) ScheduleFunc(function func() error, cadence time.Duration) (string, error) {
	task := simpleTask{function}
	jobID := xid.New().String()

	job := Job{
		Tasks:    []Task{task},
		Cadence:  cadence,
		ID:       jobID,
		NextExec: time.Now().Add(cadence),
	}

	return jobID, tm.ScheduleJob(job)
}

// ScheduleJob adds a job to the TaskManager.
// The job's tasks will execute in parallel at the specified cadence.
// Requirements: cadence > 0, at least one task, NextExec not older than one cadence,
// and a unique ID within the TaskManager.
func (tm *TaskManager) ScheduleJob(job Job) error {
	return tm.exec.Schedule(job)
}

// ScheduleTask takes a Task and adds it to the TaskManager in a Job. Creates and returns a
// randomized ID, used to identify the Job within the task manager.
func (tm *TaskManager) ScheduleTask(task Task, cadence time.Duration) (string, error) {
	jobID := xid.New().String()

	job := Job{
		Tasks:    append([]Task(nil), []Task{task}...),
		Cadence:  cadence,
		ID:       jobID,
		NextExec: time.Now().Add(cadence),
	}

	return jobID, tm.ScheduleJob(job)
}

// ScheduleTasks takes a slice of Task and adds them to the TaskManager in a Job.
// Creates and returns a randomized ID, used to identify the Job within the task manager.
func (tm *TaskManager) ScheduleTasks(tasks []Task, cadence time.Duration) (string, error) {
	jobID := xid.New().String()

	// Takes a copy of the tasks, avoiding unintended consequences if the slice is modified
	job := Job{
		Tasks:    append([]Task(nil), tasks...),
		Cadence:  cadence,
		ID:       jobID,
		NextExec: time.Now().Add(cadence),
	}

	return jobID, tm.ScheduleJob(job)
}

// RemoveJob removes a job with the given ID from the TaskManager.
func (tm *TaskManager) RemoveJob(jobID string) error {
	return tm.exec.Remove(jobID)
}

// ReplaceJob replaces an existing job (matching ID) with a new one.
// The previous job's NextExec is preserved to keep the schedule unchanged.
func (tm *TaskManager) ReplaceJob(newJob Job) error {
	return tm.exec.Replace(newJob)
}

// Stop signals the TaskManager to stop processing tasks and exit.
// Note: blocks until the TaskManager, including all workers, has completely stopped.
func (tm *TaskManager) Stop() {
	tm.stopOnce.Do(func() {
		// Signal the manager to stop
		tm.cancel()

		// Stop the worker pool
		tm.exec.Stop()

		// TODO: sort out channel ownership
		// Close the remaining channel
		close(tm.errorChan)

		tm.log.Debug().Msg("TaskManager stopped")
	})
}

// setDefaultOptions sets default values for the options of the TaskManager.
func setDefaultOptions(tm *TaskManager) {
	tm.channelBufferSize = defaultBufferSize
	tm.log = zerolog.New(zerolog.Nop())
	tm.minWorkerCount = runtime.NumCPU()
	tm.scaleInterval = defaultScaleInterval
}

// New creates, initializes, starts and returns a new TaskManager. It uses default values for
// the task manager parameters unless changed by the input opts.
func New(opts ...TMOption) *TaskManager {
	ctx, cancel := context.WithCancel(context.Background())
	tm := &TaskManager{
		ctx:    ctx,
		cancel: cancel,
	}
	setDefaultOptions(tm)
	for _, opt := range opts {
		opt(tm)
	}

	tm.errorChan = make(chan error, tm.channelBufferSize)
	tm.metrics = &managerMetrics{}

	switch tm.execMode {
	// TODO: enable when implemented
	/* case ModePerJob:
	tm.exec = &jobExecutor{
		log: tm.log,
		} */
	case ModePool:
		// Intentionally fall through as ModePool is the default
		fallthrough
	default:
		tm.exec = newPoolExecutor(
			tm.ctx,
			tm.log,
			tm.errorChan,
			tm.metrics,
			tm.channelBufferSize,
			tm.minWorkerCount,
			tm.scaleInterval)
	}

	tm.log = tm.log.With().Str("pkg", "taskman").Logger()
	tm.exec.Start()

	return tm
}

// WithChannelSize sets the channel buffer size for the TaskManager. The default is 64.
func WithChannelSize(size int) TMOption {
	return func(tm *TaskManager) {
		tm.channelBufferSize = size
	}
}

// WithLogger sets the logger for the TaskManager. The default is a no-op logger.
func WithLogger(logger zerolog.Logger) TMOption {
	return func(tm *TaskManager) {
		tm.log = logger
	}
}

// WithMinWorkerCount sets the minimum number of workers for the TaskManager. The default is the
// number of CPU cores found on the system at runtime.
func WithMinWorkerCount(count int) TMOption {
	return func(tm *TaskManager) {
		tm.minWorkerCount = count
	}
}

// WithScaleInterval sets the interval at which the worker pool is scaled for the TaskManager.
// The default is 1 minute.
func WithScaleInterval(interval time.Duration) TMOption {
	return func(tm *TaskManager) {
		tm.scaleInterval = interval
	}
}
