package taskman

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/rs/zerolog"
)

// onDemandExecutor is an implementation of executor that uses a priority queue for jobs
// and spawns a one-hit goroutine per job execution to run the tasks.
type onDemandExecutor struct {
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

	// Channels for metrics and errors
	errCh        chan error
	taskExecChan chan time.Duration
	jobExecChan  chan struct{}
	metrics      *executorMetrics

	// Synchronization for executing jobs
	executingJobs sync.WaitGroup // Tracks currently executing jobs

	// Configurable options
	channelBufferSize int
	catchUpMax        int  // max immediate catch-ups per tick when behind schedule
	parallel          bool // run tasks in parallel within each job
	maxPar            int  // parallelism limit per job (0 = unlimited)
}

// Job returns the job with the given ID.
func (e *onDemandExecutor) Job(jobID string) (Job, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	jobIndex, err := e.jobQueue.JobInQueue(jobID)
	if err != nil {
		return Job{}, fmt.Errorf("job with ID %s not found", jobID)
	}
	job := *e.jobQueue[jobIndex]
	return job, nil
}

// Metrics returns the metrics for the on-demand executor.
func (e *onDemandExecutor) Metrics() TaskManagerMetrics {
	snap := e.metrics.snapshot()
	return TaskManagerMetrics{
		ManagedJobs:          int(snap.JobsManaged),
		JobsPerSecond:        snap.JobsPerSecond,
		JobsTotalExecutions:  int(snap.JobsTotalExecutions),
		ManagedTasks:         int(snap.TasksManaged),
		TasksPerSecond:       snap.TasksPerSecond,
		TasksAverageExecTime: snap.TasksAverageExecTime,
		TasksTotalExecutions: int(snap.TasksTotalExecutions),
		PoolMetrics:          nil,
	}
}

// Remove removes a job from the queue.
func (e *onDemandExecutor) Remove(jobID string) error {
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

	// Update metrics
	e.metrics.updateMetrics(-1, -len(job.Tasks), job.Cadence)

	return nil
}

// Replace replaces a job in the queue.
func (e *onDemandExecutor) Replace(job Job) error {
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

	return nil
}

// Schedule schedules a job for execution.
func (e *onDemandExecutor) Schedule(job Job) error {
	if err := job.Validate(); err != nil {
		return fmt.Errorf("invalid job: %w", err)
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	// Validate job ID duplicity
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

	// Update metrics
	e.metrics.updateMetrics(1, len(job.Tasks), job.Cadence)

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

// Start starts the executor by setting up the job queue and channels.
func (e *onDemandExecutor) Start() {
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

	// Channels
	e.runDone = make(chan struct{})
	e.newJobChan = make(chan bool, 2)

	go e.metrics.consumeTaskExecChan(e.taskExecChan)
	go e.metrics.consumeJobExecChan(e.jobExecChan)
	go e.run()
}

// Stop signals the executor to stop processing tasks and exit.
func (e *onDemandExecutor) Stop() {
	e.stopOnce.Do(func() {
		// Signal cancellation
		e.cancel()

		// Close newJobChan to unblock run loop when queue is empty
		close(e.newJobChan)

		// Wait for run loop to exit
		<-e.runDone

		// Wait for all executing jobs to complete before closing channels
		e.executingJobs.Wait()

		// Close channels
		close(e.taskExecChan)
		close(e.jobExecChan)

		// Stop metrics
		e.metrics.cancel()

		e.log.Debug().Msg("OnDemandExecutor stopped")
	})
}

// run runs the main loop of the on-demand executor.
func (e *onDemandExecutor) run() {
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
			// Dispatch without holding lock: start a goroutine to execute the job
			e.log.Trace().Msgf("Dispatching job %s", jobID)
			e.executingJobs.Add(1)
			go func() {
				defer e.executingJobs.Done()
				e.executeJob(jobID, tasks, e.errCh, e.taskExecChan)
			}()

			// Signal that a job has been executed
			e.jobExecChan <- struct{}{}

			// Reschedule the job under lock, with catch-up
			e.mu.Lock()
			if index < len(e.jobQueue) && e.jobQueue[index].ID == jobID {
				// Advance "next" forward by whole cadences until it lands in the future,
				// but cap the number of immediate catch-ups to catchUpMax.
				skips := 0
				catchUpMax := e.catchUpMax
				if catchUpMax <= 0 {
					catchUpMax = 1
				}
				for {
					nextExec = nextExec.Add(cadence)
					if skips >= catchUpMax || nextExec.After(time.Now()) {
						break
					}
					skips++
				}
				e.jobQueue[index].NextExec = nextExec
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

// executeJob executes the tasks in a job using a one-hit goroutine.
func (e *onDemandExecutor) executeJob(jobID string, tasks []Task, errCh chan<- error, taskExecChan chan<- time.Duration) {
	if e.parallel {
		e.runParallel(jobID, tasks, errCh, taskExecChan)
		return
	}
	e.runSequential(jobID, tasks, errCh, taskExecChan)
}

// runSequential runs the job sequentially.
func (e *onDemandExecutor) runSequential(jobID string, tasks []Task, errCh chan<- error, taskExecChan chan<- time.Duration) {
	for _, t := range tasks {
		safeExecuteTask(e.ctx, jobID, t, errCh, taskExecChan)
	}
}

// runParallel runs the job in parallel.
func (e *onDemandExecutor) runParallel(jobID string, tasks []Task, errCh chan<- error, taskExecChan chan<- time.Duration) {
	var wg sync.WaitGroup
	var sem chan struct{}

	if e.maxPar > 0 {
		sem = make(chan struct{}, e.maxPar)
	}

	for _, t := range tasks {
		if e.ctx.Err() != nil {
			break
		}
		if sem != nil {
			sem <- struct{}{}
		}
		wg.Add(1)
		go func(tt Task) {
			defer wg.Done()
			safeExecuteTask(e.ctx, jobID, tt, errCh, taskExecChan)
			if sem != nil {
				<-sem
			}
		}(t)
	}

	wg.Wait()
}

// newOnDemandExecutor creates a new on-demand executor.
func newOnDemandExecutor(
	parent context.Context,
	logger zerolog.Logger,
	errCh chan error,
	metrics *executorMetrics,
	channelBufferSize int,
	catchUpMax int,
	parallel bool,
	maxPar int,
) *onDemandExecutor {
	ctx, cancel := context.WithCancel(parent)
	log := logger.With().Str("component", "executor").Logger()

	return &onDemandExecutor{
		log:          log,
		ctx:          ctx,
		cancel:       cancel,
		errCh:        errCh,
		taskExecChan: make(chan time.Duration, channelBufferSize),
		jobExecChan:  make(chan struct{}, channelBufferSize),
		metrics:      metrics,
		catchUpMax:   catchUpMax,
		parallel:     parallel,
		maxPar:       maxPar,
	}
}
