package taskman

import (
	"container/heap"
	"context"
	"errors"
	"math"
	"sync"
	"time"

	"github.com/rs/xid"
	"github.com/rs/zerolog/log"
)

var (
	// ErrManagerStopped is returned when a task is added to a stopped manager.
	ErrManagerStopped = errors.New("task manager is stopped")
	// ErrDuplicateJobID is returned when a duplicate job ID is found.
	ErrDuplicateJobID = errors.New("duplicate job ID")
	// ErrInvalidCadence is returned when a job has an invalid cadence.
	ErrInvalidCadence = errors.New("job cadence must be greater than 0")
	// ErrJobNotFound is returned when a job is not found.
	ErrJobNotFound = errors.New("job not found")
	// ErrNoTasks is returned when a job has no tasks.
	ErrNoTasks = errors.New("job has no tasks")
	// ErrZeroNextExec is returned when a job has a zero NextExec time.
	ErrZeroNextExec = errors.New("job NextExec time must be non-zero")
)

// TaskManager manages task scheduling and execution. Tasks are scheduled within Jobs, and the
// manager dispatches scheduled jobs to a worker pool for execution.
type TaskManager struct {
	sync.RWMutex

	// Queue
	jobQueue   priorityQueue // A priority queue to hold the scheduled jobs
	newJobChan chan bool     // Channel to signal that new tasks have entered the queue

	// Context and operations
	ctx      context.Context    // Context for the task manager
	cancel   context.CancelFunc // Cancel function for the task manager
	metrics  *ManagerMetrics    // Metrics for the task manager
	runDone  chan struct{}      // Channel to signal run has stopped
	stopOnce sync.Once          // Ensures Stop is only called once

	// Worker pool
	workerPool     *workerPool
	workerPoolDone chan struct{} // Channel to receive signal that the worker pool has stopped
	errorChan      chan error    // Channel to receive errors from the worker pool
	taskChan       chan Task     // Channel to send tasks to the worker pool
	minWorkerCount int32         // Minimum number of workers in the pool
	scaleInterval  time.Duration // Interval for automatic scaling of the worker pool
}

// Task is an interface for tasks that can be executed.
type Task interface {
	Execute() error
}

// BasicTask is a task that executes a function.
type BasicTask struct {
	Function func() error
}

// Execute executes the function and returns the error.
func (f BasicTask) Execute() error {
	err := f.Function()
	return err
}

// Job is responsible for a group of tasks, and when to execute them.
type Job struct {
	Cadence time.Duration
	Tasks   []Task

	ID       string
	NextExec time.Time

	index int // Index within the heap
}

// ErrorChannel returns a read-only channel for consuming errors from task execution.
func (tm *TaskManager) ErrorChannel() <-chan error {
	return tm.errorChan
}

// ScheduleFunc takes a function and adds it to the TaskManager in a Job. Creates and returns a
// randomized ID, used to identify the Job within the task manager.
// Note: wraps the function in a BasicTask.
func (tm *TaskManager) ScheduleFunc(function func() error, cadence time.Duration) (string, error) {
	task := BasicTask{function}
	jobID := xid.New().String()

	job := Job{
		Tasks:    []Task{task},
		Cadence:  cadence,
		ID:       jobID,
		NextExec: time.Now().Add(cadence),
	}

	return jobID, tm.ScheduleJob(job)
}

// ScheduleJob adds a job to the TaskManager. A job is a group of tasks that are scheduled to execute
// together. The function returns a job ID that can be used to identify the job within the TaskManager.
// Job requirements:
// - Cadence must be greater than 0
// - Job must have at least one task
// - NextExec must be non-zero and positive
// - Job must have an ID, unique within the TaskManager
func (tm *TaskManager) ScheduleJob(job Job) error {
	tm.Lock()
	defer tm.Unlock()

	// Validate the job
	err := tm.validateJob(job)
	if err != nil {
		return err
	}
	log.Debug().Msgf("Adding job with %d tasks with ID '%s' and cadence %v", len(job.Tasks), job.ID, job.Cadence)

	// Check if the task manager is stopped
	select {
	case <-tm.ctx.Done():
		// If the manager is stopped, do not continue adding the job
		log.Debug().Msg("TaskManager is stopped, not adding job")
		return ErrManagerStopped
	default:
		// Do nothing if the manager isn't stopped
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
		log.Debug().Msg("TaskManager is stopped, not signaling new task")
	default:
		select {
		case tm.newJobChan <- true:
			log.Trace().Msg("Signaled new job added")
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

// ScheduleTasks takes a slice of Task and adds them to the TaskManager in a Job. Creates and returns a
// randomized ID, used to identify the Job within the task manager.
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

	jobIndex, err := tm.jobQueue.JobInQueue(jobID)
	if err != nil {
		log.Debug().Msgf("Job with ID %s not found", jobID)
		return ErrJobNotFound
	}
	job := tm.jobQueue[jobIndex]

	taskCount := len(job.Tasks)
	log.Debug().Msgf("Removing job with ID %s and %d tasks", jobID, taskCount)

	err = tm.jobQueue.RemoveByID(jobID)
	if err != nil {
		return err
	}

	// Update task metrics
	newWidestJob := 0
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

// ReplaceJob replaces a job in the TaskManager's queue with a new job, based on their ID:s matching.
// The new job's NextExec will be overwritten by the old job's, to preserve the TaskManager's schedule.
func (tm *TaskManager) ReplaceJob(newJob Job) error {
	tm.Lock()
	defer tm.Unlock()

	jobIndex, err := tm.jobQueue.JobInQueue(newJob.ID)
	if err != nil {
		return ErrJobNotFound
	}
	log.Debug().Msgf("Replacing job with ID: %s", newJob.ID)
	oldJob := tm.jobQueue[jobIndex]
	newJob.NextExec = oldJob.NextExec
	newJob.index = oldJob.index
	tm.jobQueue[jobIndex] = &newJob
	return nil
}

// Stop signals the TaskManager to stop processing tasks and exit.
// Note: blocks until the TaskManager, including all workers, has completely stopped.
func (tm *TaskManager) Stop() {
	log.Debug().Msg("Attempting TaskManager stop")
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

		log.Debug().Msg("TaskManager stopped")
	})
}

// jobsInQueue returns the length of the jobQueue slice.
func (tm *TaskManager) jobsInQueue() int {
	tm.Lock()
	defer tm.Unlock()

	return tm.jobQueue.Len()
}

// run runs the TaskManager.
func (tm *TaskManager) run() {
	defer func() {
		log.Debug().Msg("TaskManager run loop exiting")
		close(tm.runDone)
	}()
	for {
		tm.Lock()
		if tm.jobQueue.Len() == 0 {
			tm.Unlock()
			select {
			case <-tm.newJobChan:
				log.Trace().Msg("New job added, checking for next job")
				continue
			case <-tm.ctx.Done():
				log.Info().Msg("TaskManager received stop signal, exiting run loop")
				return
			}
		} else {
			nextJob := tm.jobQueue[0]
			now := time.Now()
			delay := nextJob.NextExec.Sub(now)
			if delay <= 0 {
				log.Debug().Msgf("Dispatching job %s", nextJob.ID)
				tasks := nextJob.Tasks
				tm.Unlock()

				// Dispatch all tasks in the job to the worker pool for execution
				for _, task := range tasks {
					select {
					case <-tm.ctx.Done():
						log.Info().Msg("TaskManager received stop signal during task dispatch, exiting run loop")
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
				log.Info().Msg("TaskManager received stop signal during wait, exiting run loop")
				return
			}
		}
	}
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
			log.Debug().Msg("Periodic worker scaling")
			// Scale the worker pool based, setting 0 workers needed immediately
			tm.scaleWorkerPool(0)
		case <-tm.ctx.Done():
			log.Info().Msg("TaskManager received stop signal, exiting periodic scaling")
			return
		}
	}
}

// scaleWorkerPool scales the worker pool based on the current job queue.
// The worker pool is scaled based on the highest of three metrics:
// - The widest job in the queue in terms of number of tasks
// - The average execution time and concurrency of tasks
// - The number of tasks in the latest job related to available workers at the moment
func (tm *TaskManager) scaleWorkerPool(workersNeededNow int) {
	log.Debug().Msg("Scaling worker pool")
	log.Debug().Msgf("- Available/running workers: %d/%d", tm.workerPool.availableWorkers(), tm.workerPool.runningWorkers())
	bufferFactor50 := 1.5
	bufferFactor100 := 2.0

	// Calculate the number of workers needed based on the widest job
	workersNeededParallelTasks := tm.metrics.maxJobWidth.Load()
	// Apply 100% buffer for parallel tasks, as this is a low predictability metric
	workersNeededParallelTasks = int32(math.Ceil(float64(workersNeededParallelTasks) * bufferFactor100))
	log.Debug().Msgf("- Job width worker need: %d", workersNeededParallelTasks)

	// Calculate the number of workers needed based on the average execution time and tasks/s
	avgExecTimeSeconds := tm.metrics.averageExecTime.Load().Seconds()
	tasksPerSecond := float64(tm.metrics.tasksPerSecond.Load())
	log.Debug().Msgf("- Avg exec time: %v, tasks/s: %f", avgExecTimeSeconds, tasksPerSecond)
	workersNeededConcurrently := int32(math.Ceil(avgExecTimeSeconds * tasksPerSecond))
	// Apply 50% buffer for concurrent tasks, as this is a more predictable metric
	workersNeededConcurrently = int32(math.Ceil(float64(workersNeededConcurrently) * bufferFactor50))
	log.Debug().Msgf("- Concurrent worker need: %d", workersNeededConcurrently)

	// Calculate the number of workers needed based on the number of tasks in the latest job
	var workersNeededImmediately int32
	if tm.workerPool.availableWorkers() < int32(workersNeededNow) {
		log.Debug().Msgf("- Tasks in latest job: %d", workersNeededNow)
		// If there are not enough workers to handle the latest job, scale up immediately
		extraWorkersNeeded := int32(workersNeededNow) - tm.workerPool.availableWorkers()
		// Apply 50% buffer for immediate tasks, as this is a more predictable metric
		workersNeededImmediately = int32(math.Ceil(float64(tm.workerPool.runningWorkers()+extraWorkersNeeded) * bufferFactor50))
	}
	log.Debug().Msgf("- Immediate worker need: %d", workersNeededImmediately)

	// Use the highest of the three metrics
	workersNeeded := workersNeededParallelTasks
	if workersNeededConcurrently > workersNeeded {
		workersNeeded = workersNeededConcurrently
	}
	if workersNeededImmediately > workersNeeded {
		workersNeeded = workersNeededImmediately
	}
	// Ensure the worker pool has at least the minimum number of workers
	if workersNeeded < int32(tm.minWorkerCount) {
		workersNeeded = int32(tm.minWorkerCount)
	}

	// Adjust the worker pool size
	scalingRequestChan := tm.workerPool.workerCountScalingChannel()
	scalingRequestChan <- workersNeeded
	log.Debug().Msgf("- Worker pool scaling request sent: %d", workersNeeded)
}

// validateJob validates a Job.
// Note: does not acquire a mutex lock for accessing the jobQueue, that is up to the caller.
func (tm *TaskManager) validateJob(job Job) error {
	// Jobs with cadence <= 0 are invalid, as such jobs would execute immediately and continuously
	// and risk overwhelming the worker pool.
	if job.Cadence <= 0 {
		return ErrInvalidCadence
	}
	// Jobs with no tasks are invalid, as they would not do anything.
	if len(job.Tasks) == 0 {
		return ErrNoTasks
	}
	// Jobs with a zero NextExec time are invalid, as they would execute immediately.
	if job.NextExec.IsZero() {
		return ErrZeroNextExec
	}
	// Job ID:s are unique, so duplicates are invalid.
	if _, ok := tm.jobQueue.JobInQueue(job.ID); ok == nil {
		return ErrDuplicateJobID
	}
	return nil
}

// newTaskManager creates, initializes, and starts a new TaskManager.
func newTaskManager(
	taskChan chan Task,
	errorChan chan error,
	execTimeChan chan time.Duration,
	minWorkerCount int,
	scaleInterval time.Duration,
	workerPoolDone chan struct{},
) *TaskManager {
	log.Debug().Msg("Creating new manager")

	// Input validation
	if taskChan == nil {
		panic("taskChan cannot be nil")
	}
	if errorChan == nil {
		panic("errorChan cannot be nil")
	}
	if execTimeChan == nil {
		panic("execTimeChan cannot be nil")
	}
	if minWorkerCount <= 0 {
		panic("initWorkerCount must be greater than 0")
	}
	if workerPoolDone == nil {
		panic("workerPoolDone cannot be nil")
	}

	ctx, cancel := context.WithCancel(context.Background())

	metrics := &ManagerMetrics{
		done: workerPoolDone,
	}
	go metrics.consumeExecTime(execTimeChan)

	workerPool := newWorkerPool(minWorkerCount, errorChan, execTimeChan, taskChan, workerPoolDone)

	tm := &TaskManager{
		ctx:            ctx,
		cancel:         cancel,
		metrics:        metrics,
		jobQueue:       make(priorityQueue, 0),
		newJobChan:     make(chan bool, 1),
		errorChan:      errorChan,
		runDone:        make(chan struct{}),
		taskChan:       taskChan,
		workerPool:     workerPool,
		workerPoolDone: workerPoolDone,
		minWorkerCount: int32(minWorkerCount),
		scaleInterval:  scaleInterval,
	}

	heap.Init(&tm.jobQueue)

	log.Debug().Msg("Starting TaskManager")
	go tm.run()
	go tm.periodicWorkerScaling()

	return tm
}

// New creates, starts and returns a new TaskManager.
func New() *TaskManager {
	channelBufferSize := 256
	taskChan := make(chan Task, channelBufferSize)
	errorChan := make(chan error, channelBufferSize)
	execTimeChan := make(chan time.Duration, channelBufferSize)
	minWorkerCount := 8
	scaleInterval := 1 * time.Minute
	workerPoolDone := make(chan struct{})

	tm := newTaskManager(taskChan, errorChan, execTimeChan, minWorkerCount, scaleInterval, workerPoolDone)

	return tm
}

// newTaskManagerCustom creates, starts and returns a new TaskManager using custom values for some of
// the task manager parameters.
func newTaskManagerCustom(minWorkerCount, channelBufferSize int, scaleInterval time.Duration) *TaskManager {
	taskChan := make(chan Task, channelBufferSize)
	errorChan := make(chan error, channelBufferSize)
	execTimeChan := make(chan time.Duration, channelBufferSize)
	workerPoolDone := make(chan struct{})
	tm := newTaskManager(taskChan, errorChan, execTimeChan, minWorkerCount, scaleInterval, workerPoolDone)
	return tm
}
