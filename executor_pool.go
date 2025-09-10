package taskman

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
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
	poolScaler     *poolScaler

	// Options
	channelBufferSize int           // Buffer size for task channels
	minWorkerCount    int           // Minimum number of workers in the pool
	scaleInterval     time.Duration // Interval for automatic scaling of the worker pool

	// Metrics
	jobExecChan chan struct{}    // Channel to signal that a job has been executed
	metrics     *executorMetrics // Metrics for the overall task manager
	maxJobWidth atomic.Int32     // Widest job in the queue in terms of number of tasks
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
// revive:disable:function-length valid exception
// revive:disable:cognitive-complexity valid exception
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
		// Check for context cancellation
		select {
		case <-e.ctx.Done():
			return
		default:
			// Do nothing if the executor is running
		}

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

			// Signal that a job has been executed
			// Note: we actually don't know the execution status here but can assume that by
			// dispatching the tasks, the job has been executed
			e.jobExecChan <- struct{}{}

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

// revive:enable:function-length
// revive:enable:cognitive-complexity

// scaleWorkerPool scales the worker pool based on the current pool state and configuration.
func (e *poolExecutor) scaleWorkerPool(workersNeededNow int) {
	now := time.Now()
	e.poolScaler.scale(now, workersNeededNow)
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
	snap := e.metrics.snapshot()
	return TaskManagerMetrics{
		ManagedJobs:          int(snap.JobsManaged),
		JobsPerSecond:        snap.JobsPerSecond,
		JobsTotalExecutions:  int(snap.JobsTotalExecutions),
		ManagedTasks:         int(snap.TasksManaged),
		TasksPerSecond:       snap.TasksPerSecond,
		TasksAverageExecTime: snap.TasksAverageExecTime,
		TasksTotalExecutions: int(snap.TasksTotalExecutions),
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
	e.metrics.updateMetrics(-1, -taskCount, job.Cadence)

	// Scale worker pool if needed
	e.scaleWorkerPool(0)

	return nil
}

// Replace replaces a job in the queue.
func (e *poolExecutor) Replace(job Job) error {
	if err := job.Validate(); err != nil {
		return fmt.Errorf("invalid job: %w", err)
	}

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

	// Executor metrics updates
	oldTasks := len(oldJob.Tasks)
	newTasks := len(job.Tasks)
	deltaTasks := newTasks - oldTasks
	if deltaTasks != 0 {
		// Jobs managed unchanged (replace), but tasks managed changes by delta
		e.metrics.updateMetrics(0, deltaTasks, job.Cadence)
	} else if oldJob.Cadence != job.Cadence {
		e.metrics.updateCadence(newTasks, oldJob.Cadence, job.Cadence)
	}

	// Widest-job updates
	currentMax := int(e.maxJobWidth.Load())
	if newTasks > currentMax {
		e.maxJobWidth.Store(int32(newTasks))
	} else if oldTasks == currentMax && newTasks < currentMax {
		// The widest job got narrower; recompute widest across queue
		widest := 0
		for _, j := range e.jobQueue {
			if l := len(j.Tasks); l > widest {
				widest = l
			}
		}
		e.maxJobWidth.Store(int32(widest))
	}

	return nil
}

// Schedule schedules a job for execution.
func (e *poolExecutor) Schedule(job Job) error {
	if err := job.Validate(); err != nil {
		return fmt.Errorf("invalid job: %w", err)
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	// Validate job ID duplicity and job requirements
	if _, ok := e.jobQueue.JobInQueue(job.ID); ok == nil {
		return errors.New("invalid job: duplicate job ID")
	}

	// Check executor context state
	select {
	case <-e.ctx.Done():
		// If the executor is stopped, do not continue adding the job
		return errors.New("executor context is done")
	default:
		// Pass through if the executor is running
	}

	e.log.Debug().Msgf(
		"Scheduling job with %d tasks with ID '%s' and cadence %v",
		len(job.Tasks), job.ID, job.Cadence,
	)

	// Set NextExec to now if it is not set
	if job.NextExec.IsZero() {
		job.NextExec = time.Now().Add(job.Cadence)
	}

	// Update task metrics
	taskCount := len(job.Tasks)
	if taskCount > int(e.maxJobWidth.Load()) {
		e.maxJobWidth.Store(int32(taskCount))
	}
	e.metrics.updateMetrics(1, taskCount, job.Cadence)

	// Scale worker pool if needed
	e.scaleWorkerPool(taskCount)

	// Push the job to the queue
	heap.Push(&e.jobQueue, &job)

	// Signal the executor to check for new tasks
	select {
	case <-e.ctx.Done():
		// Do nothing if the executor is stopped
		return errors.New("executor context is done")
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
	// Metrics: reuse provided metrics but validate context
	if e.metrics == nil {
		e.metrics = newExecutorMetrics()
	} else if e.metrics.ctx == nil {
		ctx, cancel := context.WithCancel(context.Background())
		e.metrics.ctx = ctx
		e.metrics.cancel = cancel
	}

	// Job queue
	e.jobQueue = make(priorityQueue, 0)
	heap.Init(&e.jobQueue)

	// Channels (ownership):
	// - executor owns: taskChan, newJobChan
	// - workerPool owns: taskExecChan, workerPoolDone signaling
	// - caller owns: errorChan
	e.taskChan = make(chan Task, e.channelBufferSize)
	if e.errorChan == nil {
		// Create internal error channel if caller didn't provide one
		e.errorChan = make(chan error, e.channelBufferSize)
	}
	e.workerPoolDone = make(chan struct{})
	e.runDone = make(chan struct{})
	e.newJobChan = make(chan bool, 2)
	e.jobExecChan = make(chan struct{}, e.channelBufferSize)

	// Worker pool
	taskExecChan := make(chan time.Duration, e.channelBufferSize)
	e.workerPool = newWorkerPool(
		e.log, // Pass on logger instance
		e.errorChan,
		taskExecChan,
		e.taskChan,
		e.workerPoolDone,
		workerPoolCfg{
			initialWorkers:       e.minWorkerCount,
			maxWorkers:           e.poolScaler.cfg.MaxWorkers,
			utilizationThreshold: e.poolScaler.cfg.TargetUtilization,
			downScaleMinInterval: e.poolScaler.cfg.CooldownDown,
		},
	)

	e.poolScaler.workerPool = e.workerPool

	go e.metrics.consumeTaskExecChan(taskExecChan)
	go e.metrics.consumeJobExecChan(e.jobExecChan)
	go e.run()
	go e.periodicWorkerScaling()
}

// Stop signals the executor to stop processing tasks and exit. The executor will block until the
// run loop has exited, and the worker pool has stopped.
func (e *poolExecutor) Stop() {
	e.stopOnce.Do(func() {
		// Stop sequence and channel ownership:
		// - executor owns newJobChan and taskChan
		// - workerPool owns taskExecChan and workerPoolDone
		// - caller owns errorChan (never closed here)

		// 1) Signal cancellation to all components
		e.cancel()

		// 2) Close newJobChan to unblock run loop when queue is empty
		close(e.newJobChan)

		// 3) Wait for run loop to exit cleanly
		<-e.runDone

		// 4) Stop the worker pool and wait for it to finish
		e.workerPool.stop()
		<-e.workerPoolDone

		// 5) Stop metrics
		e.metrics.cancel()

		// 6) Close taskChan after workers have exited to avoid sends after close
		close(e.taskChan)

		// 7) Close jobExecChan
		close(e.jobExecChan)

		e.log.Debug().Msg("Executor stopped")
	})
}

// newPoolExecutor creates a new pool executor.
func newPoolExecutor(
	parentCtx context.Context,
	logger zerolog.Logger,
	errorChan chan error,
	metrics *executorMetrics,
	channelBufferSize int,
	minWorkerCount int,
	scaleInterval time.Duration,
	scalerConfig PoolScaleConfig,
) *poolExecutor {
	ctx, cancel := context.WithCancel(parentCtx)
	log := logger.With().Str("component", "executor").Logger()
	poolScaler := newPoolScaler(logger, nil, metrics, scalerConfig)

	return &poolExecutor{
		ctx:               ctx,
		cancel:            cancel,
		log:               log,
		errorChan:         errorChan,
		metrics:           metrics,
		channelBufferSize: channelBufferSize,
		minWorkerCount:    minWorkerCount,
		scaleInterval:     scaleInterval,
		poolScaler:        poolScaler,
	}
}
