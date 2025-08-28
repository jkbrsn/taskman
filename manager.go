// Package taskman provides a task scheduler to execute externally defined jobs at regular
// intervals. It supports two execution modes: a shared worker pool and per-job runners.
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
	defaultScaleInterval = 30 * time.Second
	defaultBufferSize    = 64

	defaultCatchUpMax = 1
	defaultParallel   = true
	defaultMaxPar     = 0
)

// ExecMode configures how tasks are executed.
// ModePool executes tasks using a shared worker pool; ModeDistributed executes tasks per job with
// each job running as its own worker.
const (
	ModePool ExecMode = iota
	ModeDistributed
)

// ExecMode is the execution mode for TaskManager.
type ExecMode int

// TMOption is a functional option for the TaskManager struct.
type TMOption func(*TaskManager)

// TaskManager manages task scheduling and execution. Tasks are scheduled within Jobs, and the
// manager dispatches scheduled jobs to a worker pool for execution.
type TaskManager struct {
	log zerolog.Logger

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
	channelBufferSize int // Buffer size for task channels

	// Distributed executor options
	deCatchUpMax int
	deParallel   bool
	deMaxPar     int

	// Pool executor options
	peMinWorkerCount int           // Minimum number of workers in the pool
	peScaleInterval  time.Duration // Interval for automatic scaling of the worker pool
}

// ErrorChannel returns a read-only channel for reading errors from task execution.
func (tm *TaskManager) ErrorChannel() <-chan error {
	return tm.errorChan
}

// Metrics returns a snapshot of the task manager's metrics.
func (tm *TaskManager) Metrics() TaskManagerMetrics {
	return tm.exec.Metrics()
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
	tm.log = zerolog.Nop()
	tm.channelBufferSize = defaultBufferSize

	tm.peMinWorkerCount = runtime.NumCPU()
	tm.peScaleInterval = defaultScaleInterval

	tm.deCatchUpMax = defaultCatchUpMax
	tm.deParallel = defaultParallel
	tm.deMaxPar = defaultMaxPar
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

	// Defaults for distributed executor options
	if tm.deCatchUpMax == 0 {
		tm.deCatchUpMax = 1
	}
	// default parallel true; zero-value is false, so set if untouched
	if !tm.deParallel {
		tm.deParallel = true
	}
	// deMaxPar: 0 means unlimited; keep as zero unless explicitly set

	tm.errorChan = make(chan error, tm.channelBufferSize)
	tm.metrics = &managerMetrics{}

	switch tm.execMode {
	case ModeDistributed:
		tm.exec = newDistributedExecutor(
			tm.ctx,
			tm.log,
			tm.errorChan,
			tm.metrics,
			tm.deCatchUpMax,
			tm.deParallel,
			tm.deMaxPar,
		)
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
			tm.peMinWorkerCount,
			tm.peScaleInterval)
	}

	tm.log = tm.log.With().Str("pkg", "taskman").Logger()
	tm.exec.Start()

	return tm
}

// WithChannelSize sets the channel buffer size for the TaskManager. The default is 64.
func WithChannelSize(size int) TMOption {
	return func(tm *TaskManager) { tm.channelBufferSize = size }
}

// WithLogger sets the logger for the TaskManager. The default is a no-op logger.
func WithLogger(logger zerolog.Logger) TMOption {
	return func(tm *TaskManager) { tm.log = logger }
}

// WithMode sets the execution mode for the TaskManager. The default is ModePool.
func WithMode(mode ExecMode) TMOption {
	return func(tm *TaskManager) { tm.execMode = mode }
}

// WithMPMinWorkerCount (ModePool setting) sets the minimum number of workers for the
// TaskManager's worker pool. The default is the number of CPU cores found on the system at
// runtime.
func WithMPMinWorkerCount(count int) TMOption {
	return func(tm *TaskManager) { tm.peMinWorkerCount = count }
}

// WithMPScaleInterval (ModePool setting) sets the interval at which the worker pool is scaled
// for the TaskManager. The default is 1 minute.
func WithMPScaleInterval(interval time.Duration) TMOption {
	return func(tm *TaskManager) { tm.peScaleInterval = interval }
}

// WithMDParallel (ModeDistributed setting) controls whether tasks inside each job execute in
// parallel (default: true).
func WithMDParallel(parallel bool) TMOption {
	return func(tm *TaskManager) { tm.deParallel = parallel }
}

// WithMDMaxParallel (ModeDistributed setting) sets max parallelism per job (0 = unlimited).
func WithMDMaxParallel(n int) TMOption {
	return func(tm *TaskManager) { tm.deMaxPar = n }
}

// WithMDCatchUpMax (ModeDistributed setting) sets how many missed cadences may run
// back-to-back when behind (default: 1).
func WithMDCatchUpMax(n int) TMOption {
	return func(tm *TaskManager) { tm.deCatchUpMax = n }
}
