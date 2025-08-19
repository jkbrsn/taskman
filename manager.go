// Package taskman provides a simple task scheduler with a worker pool.
package taskman

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"math"
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

// TMOption is a functional option for the TaskManager struct.
type TMOption func(*TaskManager)

// TaskManager manages task scheduling and execution. Tasks are scheduled within Jobs, and the
// manager dispatches scheduled jobs to a worker pool for execution.
type TaskManager struct {
	sync.RWMutex

	log zerolog.Logger

	// Queue
	jobQueue   priorityQueue // A priority queue to hold the scheduled jobs
	newJobChan chan bool     // Channel to signal that new tasks have entered the queue

	// Context and operations
	ctx      context.Context    // Context for the task manager
	cancel   context.CancelFunc // Cancel function for the task manager
	metrics  *managerMetrics    // Metrics for the task manager
	runDone  chan struct{}      // Channel to signal run has stopped
	stopOnce sync.Once          // Ensures Stop is only called once

	// Worker pool
	workerPool     *workerPool
	workerPoolDone chan struct{} // Channel to receive signal that the worker pool has stopped
	errorChan      chan error    // Channel to receive errors from the worker pool
	taskChan       chan Task     // Channel to send tasks to the worker pool

	// Options
	channelBufferSize int           // Buffer size for task channels
	minWorkerCount    int           // Minimum number of workers in the pool
	scaleInterval     time.Duration // Interval for automatic scaling of the worker pool
}

// jobsInQueue returns the length of the jobQueue slice.
func (tm *TaskManager) jobsInQueue() int {
	tm.Lock()
	defer tm.Unlock()

	return tm.jobQueue.Len()
}

// periodicWorkerScaling scales the worker pool at regular intervals, based on the state of the
// job queue. The worker pool is already scaled every time a job is added or removed, but this
// function provides a way to scale the worker pool over time.
func (tm *TaskManager) periodicWorkerScaling() {
	ticker := time.NewTicker(tm.scaleInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// Scale the worker pool based, setting 0 workers needed immediately
			tm.scaleWorkerPool(0)
		case <-tm.ctx.Done():
			// TaskManager received stop signal, exiting periodic scaling
			return
		}
	}
}

// run runs the main loop of TaskManager.
func (tm *TaskManager) run() {
	defer func() {
		close(tm.runDone)
	}()
	for {
		tm.Lock()
		if tm.jobQueue.Len() == 0 {
			tm.Unlock()
			select {
			case <-tm.newJobChan:
				// New job added, checking for next job
				continue
			case <-tm.ctx.Done():
				// TaskManager received stop signal, exiting run loop
				return
			}
		} else {
			nextJob := tm.jobQueue[0]
			now := time.Now()
			delay := nextJob.NextExec.Sub(now)
			if delay <= 0 {
				tm.log.Trace().Msgf("Dispatching job %s", nextJob.ID)
				tasks := nextJob.Tasks
				tm.Unlock()

				// Dispatch all tasks in the job to the worker pool for execution
				for _, task := range tasks {
					select {
					case <-tm.ctx.Done():
						// TaskManager received stop signal during task dispatch, exiting run loop
						return
					case tm.taskChan <- task:
						// Successfully sent the task
					}
				}

				// Reschedule the job
				tm.Lock()
				nextJob.NextExec = nextJob.NextExec.Add(nextJob.Cadence)
				heap.Fix(&tm.jobQueue, nextJob.index)
				tm.Unlock()
				continue
			}
			tm.Unlock()

			// Wait until the next job is due or until stopped.
			select {
			case <-time.After(delay):
				// Time to execute the next job
				continue
			case <-tm.newJobChan:
				// A new job was added, check for the next job
				continue
			case <-tm.ctx.Done():
				// TaskManager received stop signal during wait, exiting run loop
				return
			}
		}
	}
}

// scaleWorkerPool scales the worker pool based on the current job queue.
// The worker pool is scaled based on the highest of three metrics:
// - The widest job in the queue in terms of number of tasks
// - The average execution time and concurrency of tasks
// - The number of tasks in the latest job related to available workers at the moment
func (tm *TaskManager) scaleWorkerPool(workersNeededNow int) {
	tm.log.Debug().Msgf(
		"Scaling workers, available/running: %d/%d",
		tm.workerPool.availableWorkers(), tm.workerPool.runningWorkers(),
	)
	const bufferFactor50 = 1.5
	bufferFactor100 := 2.0

	// Calculate the number of workers needed based on the widest job
	workersNeededParallelTasks := tm.metrics.maxJobWidth.Load()
	// Apply the larger buffer factor for parallel tasks, as this is a low predictability metric
	workersNeededParallelTasks = int32(
		math.Ceil(float64(workersNeededParallelTasks) * bufferFactor100),
	)

	// Calculate the number of workers needed based on the average execution time and tasks/s
	avgExecTimeSeconds := tm.metrics.averageExecTime.Load().Seconds()
	tasksPerSecond := float64(tm.metrics.tasksPerSecond.Load())
	workersNeededConcurrently := int32(math.Ceil(avgExecTimeSeconds * tasksPerSecond))
	// Apply the smaller buffer factor for concurrent tasks, as this is a more predictable metric
	workersNeededConcurrently = int32(
		math.Ceil(float64(workersNeededConcurrently) * bufferFactor50),
	)

	// Calculate the number of workers needed right now
	var workersNeededImmediately int32
	if tm.workerPool.availableWorkers() < int32(workersNeededNow) {
		// If there are not enough workers to handle the incoming job, scale up immediately
		extraWorkersNeeded := int32(workersNeededNow) - tm.workerPool.availableWorkers()
		// Apply the smaller buffer factor for immediate tasks, as this is a more predictable metric
		workersNeededImmediately = int32(
			math.Ceil(
				float64(tm.workerPool.runningWorkers()+extraWorkersNeeded) * bufferFactor50,
			),
		)
	}

	// Use the highest of the three metrics
	workersNeeded := max(
		workersNeededParallelTasks,
		workersNeededConcurrently,
		workersNeededImmediately,
	)
	// Ensure the worker pool has at least the minimum number of workers
	workersNeeded = max(workersNeeded, int32(tm.minWorkerCount))
	// Ensure the worker pool has at most the maximum number of workers
	workersNeeded = min(workersNeeded, int32(maxWorkerCount))

	// Adjust the worker pool size
	tm.workerPool.enqueueWorkerScaling(workersNeeded)
	tm.log.Debug().Msgf("Scaling workers, request: %d", workersNeeded)
}

// start initializes metrics, creates the job queue and the channels, sets up the worker pool, and
// then starts the task manager.
func (tm *TaskManager) start() {
	// Metrics
	tm.metrics = &managerMetrics{
		done: tm.workerPoolDone,
	}

	// Job queue
	tm.jobQueue = make(priorityQueue, 0)
	heap.Init(&tm.jobQueue)

	// Channels
	tm.taskChan = make(chan Task, tm.channelBufferSize)
	tm.errorChan = make(chan error, tm.channelBufferSize)
	tm.workerPoolDone = make(chan struct{})
	tm.runDone = make(chan struct{})
	tm.newJobChan = make(chan bool, 2)

	// Worker pool
	tm.workerPool = newWorkerPool(
		tm.log, // Pass on logger instance
		tm.minWorkerCount,
		tm.errorChan,
		make(chan time.Duration, tm.channelBufferSize),
		tm.taskChan,
		tm.workerPoolDone,
	)

	go tm.metrics.consumeExecTime(tm.workerPool.execTimeChan)
	go tm.run()
	go tm.periodicWorkerScaling()
}

// ErrorChannel returns a read-only channel for reading errors from task execution.
func (tm *TaskManager) ErrorChannel() <-chan error {
	return tm.errorChan
}

// Metrics returns a snapshot of the task manager's metrics.
func (tm *TaskManager) Metrics() TaskManagerMetrics {
	tm.RLock()
	defer tm.RUnlock()

	jobsInQueue := tm.jobQueue.Len()

	metrics := TaskManagerMetrics{
		QueuedJobs:           jobsInQueue,
		QueuedTasks:          int(tm.metrics.tasksInQueue.Load()),
		QueueMaxJobWidth:     int(tm.metrics.maxJobWidth.Load()),
		TaskAverageExecTime:  tm.metrics.averageExecTime.Load(),
		TasksTotalExecutions: int(tm.metrics.totalTaskExecutions.Load()),
		TasksPerSecond:       tm.metrics.tasksPerSecond.Load(),
		WorkerCountTarget:    int(tm.workerPool.workerCountTarget.Load()),
		WorkerScalingEvents:  int(tm.workerPool.workerScalingEvents.Load()),
		WorkerUtilization:    float32(tm.workerPool.utilization()),
		WorkersActive:        int(tm.workerPool.workersActive.Load()),
		WorkersRunning:       int(tm.workerPool.workersRunning.Load()),
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

// ScheduleJob adds a job to the TaskManager. A job is a group of tasks that are scheduled to
// execute at a regular interval. The tasks in the job are executed in parallel, but the job's
// cadence determines when the job is executed. The function returns a job ID that can be used
// to identify the job within the TaskManager.
// Job requirements:
// - Cadence must be greater than 0
// - Job must have at least one task
// - NextExec must not be more than one cadence old, set to time.Now() for instant execution
// - Job must have an ID, unique within the TaskManager
func (tm *TaskManager) ScheduleJob(job Job) error {
	tm.Lock()
	defer tm.Unlock()

	// Validate job ID duplicity and job requirements
	if _, ok := tm.jobQueue.JobInQueue(job.ID); ok == nil {
		return errors.New("invalid job: duplicate job ID")
	}
	if err := job.Validate(); err != nil {
		return fmt.Errorf("invalid job: %w", err)
	}

	tm.log.Debug().Msgf(
		"Scheduling job with %d tasks with ID '%s' and cadence %v",
		len(job.Tasks), job.ID, job.Cadence,
	)

	// Check task manager state
	select {
	case <-tm.ctx.Done():
		// If the manager is stopped, do not continue adding the job
		return errors.New("task manager is stopped")
	default:
		// Passthrough if the manager is running
	}

	// Update task metrics
	taskCount := len(job.Tasks)
	tm.metrics.updateTaskMetrics(taskCount, job.Cadence)

	// Scale worker pool if needed
	tm.scaleWorkerPool(taskCount)

	// Push the job to the queue
	heap.Push(&tm.jobQueue, &job)

	// Signal the task manager to check for new tasks
	select {
	case <-tm.ctx.Done():
		// Do nothing if the manager is stopped
		return errors.New("task manager is stopped")
	default:
		select {
		case tm.newJobChan <- true:
			tm.log.Trace().Msg("Signaled new job added")
		default:
			// Do nothing if no one is listening
		}
	}

	return nil
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

// RemoveJob removes a job from the TaskManager.
func (tm *TaskManager) RemoveJob(jobID string) error {
	tm.Lock()
	defer tm.Unlock()

	// Get the job from the queue
	jobIndex, err := tm.jobQueue.JobInQueue(jobID)
	if err != nil {
		return fmt.Errorf("job with ID %s not found", jobID)
	}
	job := tm.jobQueue[jobIndex]

	// Remove the job from the queue
	err = tm.jobQueue.RemoveByID(jobID)
	if err != nil {
		return err
	}

	// Update task metrics
	newWidestJob := 0
	taskCount := len(job.Tasks)
	if taskCount == int(tm.metrics.maxJobWidth.Load()) {
		// If the removed job is widest, find the second widest job in the queue
		for _, j := range tm.jobQueue {
			// If another job has the same number of tasks, keep the widest job at the same value
			if len(j.Tasks) == taskCount && j.ID != jobID {
				newWidestJob = taskCount
				break
			}
			// Otherwise, find the second widest job
			if len(j.Tasks) > newWidestJob && len(j.Tasks) < taskCount {
				newWidestJob = len(j.Tasks)
			}
		}
		tm.metrics.maxJobWidth.Store(int32(newWidestJob))
	}
	// Update the task metrics with a negative task count to signify removal
	tm.metrics.updateTaskMetrics(-taskCount, job.Cadence)

	// Scale worker pool if needed
	tm.scaleWorkerPool(0)

	return nil
}

// ReplaceJob replaces a job in the TaskManager's queue with a new job, if their ID:s match. The
// new job's NextExec will be overwritten by the old job's, to preserve the TaskManager's schedule.
// Use this function to update a job's tasks without changing its schedule.
func (tm *TaskManager) ReplaceJob(newJob Job) error {
	tm.Lock()
	defer tm.Unlock()

	// Get the job's index in the queue
	jobIndex, err := tm.jobQueue.JobInQueue(newJob.ID)
	if err != nil {
		return errors.New("job not found")
	}

	// Replace the job in the queue
	oldJob := tm.jobQueue[jobIndex]
	newJob.NextExec = oldJob.NextExec
	newJob.index = oldJob.index
	tm.jobQueue[jobIndex] = &newJob
	return nil
}

// Stop signals the TaskManager to stop processing tasks and exit.
// Note: blocks until the TaskManager, including all workers, has completely stopped.
func (tm *TaskManager) Stop() {
	tm.stopOnce.Do(func() {
		// Signal the manager to stop
		tm.cancel()

		// Stop the worker pool
		tm.workerPool.stop()

		// Wait for the run loop to exit, and the worker pool to stop
		<-tm.runDone
		<-tm.workerPoolDone

		// Close the remaining channels
		close(tm.newJobChan)
		close(tm.errorChan)
		close(tm.taskChan)

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

	tm.log = tm.log.With().Str("pkg", "taskman").Logger()
	tm.start()

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
