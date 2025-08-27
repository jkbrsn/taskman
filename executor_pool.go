// Package taskman provides a simple task scheduler with a worker pool.
package taskman

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
)

// poolExecutor is an implementation of executor that uses a worker pool to execute tasks.
type poolExecutor struct {
	log zerolog.Logger

	ctx    context.Context
	cancel context.CancelFunc

	// Queue
	mu         sync.RWMutex
	jobQueue   priorityQueue // A priority queue to hold the scheduled jobs
	newJobChan chan bool     // Channel to signal that new tasks have entered the queue

	// Operations
	runDone  chan struct{} // Channel to signal run has stopped
	stopOnce sync.Once     // Ensures Stop is only called once

	// Worker pool
	workerPool     *workerPool
	workerPoolDone chan struct{} // Channel to receive signal that the worker pool has stopped
	errorChan      chan error    // Channel to receive errors from the worker pool
	taskChan       chan Task     // Channel to send tasks to the worker pool

	// Options
	channelBufferSize int           // Buffer size for task channels
	minWorkerCount    int           // Minimum number of workers in the pool
	scaleInterval     time.Duration // Interval for automatic scaling of the worker pool

	// Metrics
	metrics     *managerMetrics // Metrics for the overall task manager
	maxJobWidth atomic.Int32    // Widest job in the queue in terms of number of tasks
}

// periodicWorkerScaling scales the worker pool at regular intervals, based on the state of the
// job queue. The worker pool is already scaled every time a job is added or removed, but this
// function provides a way to scale the worker pool over time.
func (e *poolExecutor) periodicWorkerScaling() {
	ticker := time.NewTicker(e.scaleInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// Scale the worker pool based, setting 0 workers needed immediately
			e.scaleWorkerPool(0)
		case <-e.ctx.Done():
			// TaskManager received stop signal, exiting periodic scaling
			return
		}
	}
}

// run runs the main loop of the pool executor.
func (e *poolExecutor) run() {
	defer close(e.runDone)

	// Reusable timer to avoid goroutine churn from time.After
	var (
		timer *time.Timer
		fires <-chan time.Time
	)

	stopTimer := func() {
		if timer != nil {
			if !timer.Stop() {
				// Drain if already fired
				select {
				case <-timer.C:
				default:
				}
			}
		}
		fires = nil
	}

	for {
		// Snapshot only what's needed under lock
		e.mu.Lock()
		queueLen := e.jobQueue.Len()
		var (
			jobID    string
			tasks    []Task
			nextExec time.Time
			cadence  time.Duration
			index    int
		)
		if queueLen > 0 {
			next := e.jobQueue[0]
			jobID = next.ID
			tasks = next.Tasks
			nextExec = next.NextExec
			cadence = next.Cadence
			index = next.index
		}
		e.mu.Unlock()

		if queueLen == 0 {
			// No jobs: wait for new job or stop
			stopTimer()
			select {
			case <-e.newJobChan:
				continue
			case <-e.ctx.Done():
				return
			}
		}

		now := time.Now()
		delay := nextExec.Sub(now)
		if delay <= 0 {
			// Dispatch without holding lock
			e.log.Trace().Msgf("Dispatching job %s", jobID)
			for _, task := range tasks {
				select {
				case <-e.ctx.Done():
					return
				case e.taskChan <- task:
				}
			}

			// Reschedule the job under lock
			e.mu.Lock()
			if index < len(e.jobQueue) && e.jobQueue[index].ID == jobID {
				e.jobQueue[index].NextExec = nextExec.Add(cadence)
				heap.Fix(&e.jobQueue, index)
			}
			e.mu.Unlock()
			continue
		}

		// Wait until due, but reuse timer so we can preempt on new jobs/stop
		if timer == nil {
			timer = time.NewTimer(delay)
			fires = timer.C
		} else {
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(delay)
			fires = timer.C
		}

		select {
		case <-fires:
			// Time to execute next job
			continue
		case <-e.newJobChan:
			// New job added; re-evaluate queue head
			continue
		case <-e.ctx.Done():
			return
		}
	}
}

// scaleWorkerPool scales the worker pool based on the current job queue.
// The worker pool is scaled based on the highest of three metrics:
// - The widest job in the queue in terms of number of tasks
// - The average execution time and concurrency of tasks
// - The number of tasks in the latest job related to available workers at the moment
func (e *poolExecutor) scaleWorkerPool(workersNeededNow int) {
	e.log.Debug().Msgf(
		"Scaling workers, available/running: %d/%d",
		e.workerPool.availableWorkers(), e.workerPool.runningWorkers(),
	)
	const bufferFactor50 = 1.5
	bufferFactor100 := 2.0

	// Calculate the number of workers needed based on the widest job
	workersNeededParallelTasks := e.maxJobWidth.Load()
	// Apply the larger buffer factor for parallel tasks, as this is a low predictability metric
	workersNeededParallelTasks = int32(
		math.Ceil(float64(workersNeededParallelTasks) * bufferFactor100),
	)

	// Calculate the number of workers needed based on the average execution time and tasks/s
	avgExecTimeSeconds := e.metrics.averageExecTime.Load().Seconds()
	tasksPerSecond := float64(e.metrics.tasksPerSecond.Load())
	workersNeededConcurrently := int32(math.Ceil(avgExecTimeSeconds * tasksPerSecond))
	// Apply the smaller buffer factor for concurrent tasks, as this is a more predictable metric
	workersNeededConcurrently = int32(
		math.Ceil(float64(workersNeededConcurrently) * bufferFactor50),
	)

	// Calculate the number of workers needed right now
	var workersNeededImmediately int32
	if e.workerPool.availableWorkers() < int32(workersNeededNow) {
		// If there are not enough workers to handle the incoming job, scale up immediately
		extraWorkersNeeded := int32(workersNeededNow) - e.workerPool.availableWorkers()
		// Apply the smaller buffer factor for immediate tasks, as this is a more predictable metric
		workersNeededImmediately = int32(
			math.Ceil(
				float64(e.workerPool.runningWorkers()+extraWorkersNeeded) * bufferFactor50,
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
	workersNeeded = max(workersNeeded, int32(e.minWorkerCount))
	// Ensure the worker pool has at most the maximum number of workers
	workersNeeded = min(workersNeeded, int32(maxWorkerCount))

	// Adjust the worker pool size
	e.workerPool.enqueueWorkerScaling(workersNeeded)
	e.log.Debug().Msgf("Scaling workers, request: %d", workersNeeded)
}

// Job returns the job with the given ID.
func (e *poolExecutor) Job(jobID string) (Job, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	jobIndex, err := e.jobQueue.JobInQueue(jobID)
	if err != nil {
		return Job{}, fmt.Errorf("job with ID %s not found", jobID)
	}
	job := *e.jobQueue[jobIndex]
	return job, nil
}

// Metrics returns the metrics for the executor.
func (e *poolExecutor) Metrics() TaskManagerMetrics {
	return TaskManagerMetrics{
		ManagedJobs:          int(e.metrics.jobsManaged.Load()),
		JobsPerSecond:        e.metrics.jobsPerSecond.Load(),
		ManagedTasks:         int(e.metrics.tasksManaged.Load()),
		TasksPerSecond:       e.metrics.tasksPerSecond.Load(),
		TaskAverageExecTime:  time.Duration(e.metrics.averageExecTime.Load()),
		TasksTotalExecutions: int(e.metrics.totalTaskExecutions.Load()),
		PoolMetrics: &PoolMetrics{
			WidestJobWidth:      int(e.maxJobWidth.Load()),
			WorkerCountTarget:   int(e.workerPool.workerCountTarget.Load()),
			WorkerScalingEvents: int(e.workerPool.workerScalingEvents.Load()),
			WorkerUtilization:   float32(e.workerPool.utilization()),
			WorkersActive:       int(e.workerPool.workersActive.Load()),
			WorkersRunning:      int(e.workerPool.workersRunning.Load()),
		},
	}
}

// Remove removes a job from the queue.
func (e *poolExecutor) Remove(jobID string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	// Get the job from the queue
	jobIndex, err := e.jobQueue.JobInQueue(jobID)
	if err != nil {
		return fmt.Errorf("job with ID %s not found", jobID)
	}
	job := e.jobQueue[jobIndex]

	// Remove the job from the queue
	err = e.jobQueue.RemoveByID(jobID)
	if err != nil {
		return err
	}

	// Update task metrics
	newWidestJob := 0
	taskCount := len(job.Tasks)
	if taskCount == int(e.maxJobWidth.Load()) {
		// If the removed job is widest, find the second widest job in the queue
		for _, j := range e.jobQueue {
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
		e.maxJobWidth.Store(int32(newWidestJob))
	}
	// Update the task metrics with a negative task count to signify removal
	e.metrics.updateTaskMetrics(-1, -taskCount, job.Cadence)

	// Scale worker pool if needed
	e.scaleWorkerPool(0)

	return nil
}

// Replace replaces a job in the queue.
func (e *poolExecutor) Replace(job Job) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	// Get the job's index in the queue
	jobIndex, err := e.jobQueue.JobInQueue(job.ID)
	if err != nil {
		return fmt.Errorf("replace job %q: %w", job.ID, err)
	}

	// Replace the job in the queue, preserving scheduling fields
	oldJob := e.jobQueue[jobIndex]
	job.NextExec = oldJob.NextExec
	job.index = oldJob.index
	e.jobQueue[jobIndex] = &job

	// Preserve heap invariants if ordering-related fields ever change
	heap.Fix(&e.jobQueue, job.index)

	return nil
}

// Schedule schedules a job for execution.
func (e *poolExecutor) Schedule(job Job) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	// Validate job ID duplicity and job requirements
	if _, ok := e.jobQueue.JobInQueue(job.ID); ok == nil {
		return errors.New("invalid job: duplicate job ID")
	}
	if err := job.Validate(); err != nil {
		return fmt.Errorf("invalid job: %w", err)
	}

	e.log.Debug().Msgf(
		"Scheduling job with %d tasks with ID '%s' and cadence %v",
		len(job.Tasks), job.ID, job.Cadence,
	)

	// Check task manager state
	select {
	case <-e.ctx.Done():
		// If the manager is stopped, do not continue adding the job
		return errors.New("task manager is stopped")
	default:
		// Pass through if the manager is running
	}

	// Update task metrics
	taskCount := len(job.Tasks)
	if taskCount > int(e.maxJobWidth.Load()) {
		e.maxJobWidth.Store(int32(taskCount))
	}
	e.metrics.updateTaskMetrics(1, taskCount, job.Cadence)

	// Scale worker pool if needed
	e.scaleWorkerPool(taskCount)

	// Push the job to the queue
	heap.Push(&e.jobQueue, &job)

	// Signal the task manager to check for new tasks
	select {
	case <-e.ctx.Done():
		// Do nothing if the manager is stopped
		return errors.New("task manager is stopped")
	default:
		select {
		case e.newJobChan <- true:
			e.log.Trace().Msg("Signaled new job added")
		default:
			// Do nothing if no one is listening
		}
	}

	return nil
}

// Start starts the executor by setting up the job queue, channels, and worker pool.
func (e *poolExecutor) Start() {
	// Metrics: reuse provided metrics and set done channel
	if e.metrics == nil {
		e.metrics = &managerMetrics{}
	}
	e.metrics.done = e.workerPoolDone

	// Job queue
	e.jobQueue = make(priorityQueue, 0)
	heap.Init(&e.jobQueue)

	// Channels
	e.taskChan = make(chan Task, e.channelBufferSize)
	// Use the provided errorChan to propagate errors to TaskManager
	if e.errorChan == nil {
		e.errorChan = make(chan error, e.channelBufferSize)
	}
	e.workerPoolDone = make(chan struct{})
	e.runDone = make(chan struct{})
	e.newJobChan = make(chan bool, 2)

	// Worker pool
	e.workerPool = newWorkerPool(
		e.log, // Pass on logger instance
		e.minWorkerCount,
		e.errorChan,
		make(chan time.Duration, e.channelBufferSize),
		e.taskChan,
		e.workerPoolDone,
	)

	go e.metrics.consumeExecTime(e.workerPool.execTimeChan)
	go e.run()
	go e.periodicWorkerScaling()
}

// Stop signals the executor to stop processing tasks and exit. The executor will block until the
// run loop has exited, and the worker pool has stopped.
func (e *poolExecutor) Stop() {
	e.stopOnce.Do(func() {
		e.cancel()

		// Stop the worker pool
		e.workerPool.stop()

		// Wait for the run loop to exit, and the worker pool to stop
		<-e.runDone
		<-e.workerPoolDone

		// Close the remaining channels
		close(e.newJobChan)
		close(e.taskChan)

		e.log.Debug().Msg("Executor stopped")
	})
}

// newPoolExecutor creates a new pool executor.
func newPoolExecutor(
	parentCtx context.Context,
	logger zerolog.Logger,
	errorChan chan error,
	metrics *managerMetrics,
	channelBufferSize int,
	minWorkerCount int,
	scaleInterval time.Duration,
) *poolExecutor {
	ctx, cancel := context.WithCancel(parentCtx)
	log := logger.With().Str("component", "executor").Logger()

	return &poolExecutor{
		ctx:               ctx,
		cancel:            cancel,
		log:               log,
		errorChan:         errorChan,
		metrics:           metrics,
		channelBufferSize: channelBufferSize,
		minWorkerCount:    minWorkerCount,
		scaleInterval:     scaleInterval,
	}
}
