package taskman

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/rs/xid"
	"github.com/rs/zerolog/log"
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
	metrics  *managerMetrics    // Metrics for the task manager
	runDone  chan struct{}      // Channel to signal run has stopped
	stopOnce sync.Once          // Ensures Stop is only called once

	// Worker pool
	workerPool     *workerPool
	workerPoolDone chan struct{} // Channel to receive signal that the worker pool has stopped
	errorChan      chan error    // Channel to receive errors from the worker pool
	taskChan       chan Task     // Channel to send tasks to the worker pool
	minWorkerCount int           // Minimum number of workers in the pool
	scaleInterval  time.Duration // Interval for automatic scaling of the worker pool
}

// Task is an interface for tasks that can be executed.
type Task interface {
	Execute() error
}

// SimpleTask is a task that executes a function.
type SimpleTask struct {
	function func() error
}

// Execute executes the function and returns the error.
func (st SimpleTask) Execute() error {
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

// ErrorChannel returns a read-only channel for reading errors from task execution.
func (tm *TaskManager) ErrorChannel() <-chan error {
	return tm.errorChan
}

// ScheduleFunc takes a function and adds it to the TaskManager in a Job. Creates and returns a
// randomized ID, used to identify the Job within the task manager.
func (tm *TaskManager) ScheduleFunc(function func() error, cadence time.Duration) (string, error) {
	task := SimpleTask{function}
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

	// Validate the job
	err := tm.validateJob(job)
	if err != nil {
		return err
	}
	log.Debug().Msgf("Scheduling job with %d tasks with ID '%s' and cadence %v", len(job.Tasks), job.ID, job.Cadence)

	// Check if the task manager is stopped
	select {
	case <-tm.ctx.Done():
		// If the manager is stopped, do not continue adding the job
		return errors.New("task manager is stopped")
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
		return errors.New("task manager is stopped")
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
				log.Trace().Msgf("Dispatching job %s", nextJob.ID)
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

// scaleWorkerPool scales the worker pool based on the current job queue.
// The worker pool is scaled based on the highest of three metrics:
// - The widest job in the queue in terms of number of tasks
// - The average execution time and concurrency of tasks
// - The number of tasks in the latest job related to available workers at the moment
func (tm *TaskManager) scaleWorkerPool(workersNeededNow int) {
	log.Debug().Msgf("Scaling workers, available/running: %d/%d", tm.workerPool.availableWorkers(), tm.workerPool.runningWorkers())
	bufferFactor50 := 1.5
	bufferFactor100 := 2.0

	// Calculate the number of workers needed based on the widest job
	workersNeededParallelTasks := tm.metrics.maxJobWidth.Load()
	// Apply the larger buffer factor for parallel tasks, as this is a low predictability metric
	workersNeededParallelTasks = int32(math.Ceil(float64(workersNeededParallelTasks) * bufferFactor100))

	// Calculate the number of workers needed based on the average execution time and tasks/s
	avgExecTimeSeconds := tm.metrics.averageExecTime.Load().Seconds()
	tasksPerSecond := float64(tm.metrics.tasksPerSecond.Load())
	workersNeededConcurrently := int32(math.Ceil(avgExecTimeSeconds * tasksPerSecond))
	// Apply the smaller buffer factor for concurrent tasks, as this is a more predictable metric
	workersNeededConcurrently = int32(math.Ceil(float64(workersNeededConcurrently) * bufferFactor50))

	// Calculate the number of workers needed right now
	var workersNeededImmediately int32
	if tm.workerPool.availableWorkers() < int32(workersNeededNow) {
		// If there are not enough workers to handle the incoming job, scale up immediately
		extraWorkersNeeded := int32(workersNeededNow) - tm.workerPool.availableWorkers()
		// Apply the smaller buffer factor for immediate tasks, as this is a more predictable metric
		workersNeededImmediately = int32(math.Ceil(float64(tm.workerPool.runningWorkers()+extraWorkersNeeded) * bufferFactor50))
	}

	// Use the highest of the three metrics
	workersNeeded := max(workersNeededParallelTasks, workersNeededConcurrently, workersNeededImmediately)
	// Ensure the worker pool has at least the minimum number of workers
	if workersNeeded < int32(tm.minWorkerCount) {
		workersNeeded = int32(tm.minWorkerCount)
	}

	// Adjust the worker pool size
	scalingRequestChan := tm.workerPool.workerCountScalingChannel()
	scalingRequestChan <- workersNeeded
	log.Debug().Msgf("Scaling workers, request: %d", workersNeeded)
}

// validateJob validates a Job.
// Note: does not acquire a mutex lock for accessing the jobQueue, that is up to the caller.
func (tm *TaskManager) validateJob(job Job) error {
	// Jobs with cadence <= 0 are invalid, as such jobs would execute immediately and continuously
	// and risk overwhelming the worker pool.
	if job.Cadence <= 0 {
		return errors.New("invalid cadence, must be greater than 0")
	}
	// Jobs with no tasks are invalid, as they would not do anything.
	if len(job.Tasks) == 0 {
		return errors.New("job has no tasks")
	}
	// Jobs with a NextExec time more than one Cadence old are invalid, as they would re-execute continually.
	if job.NextExec.Before(time.Now().Add(-job.Cadence)) {
		return errors.New("job NextExec is too early")
	}
	// Job ID:s are unique, so duplicates are invalid.
	if _, ok := tm.jobQueue.JobInQueue(job.ID); ok == nil {
		return errors.New("duplicate job ID")
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

	// Create and start the manager metrics
	metrics := &managerMetrics{
		done: workerPoolDone,
	}
	go metrics.consumeExecTime(execTimeChan)

	// Create the worker pool
	workerPool := newWorkerPool(minWorkerCount, errorChan, execTimeChan, taskChan, workerPoolDone)

	ctx, cancel := context.WithCancel(context.Background())
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
		minWorkerCount: minWorkerCount,
		scaleInterval:  scaleInterval,
	}

	heap.Init(&tm.jobQueue)

	go tm.run()
	go tm.periodicWorkerScaling()

	return tm
}

// New creates, starts and returns a new TaskManager.
func New() *TaskManager {
	channelBufferSize := 64
	taskChan := make(chan Task, channelBufferSize)
	errorChan := make(chan error, channelBufferSize)
	execTimeChan := make(chan time.Duration, channelBufferSize)
	minWorkerCount := 8
	scaleInterval := 1 * time.Minute
	workerPoolDone := make(chan struct{})

	tm := newTaskManager(taskChan, errorChan, execTimeChan, minWorkerCount, scaleInterval, workerPoolDone)

	return tm
}

// newCustom creates, starts and returns a new TaskManager using custom values for some of
// the task manager parameters.
func newCustom(minWorkerCount, channelBufferSize int, scaleInterval time.Duration) *TaskManager {
	taskChan := make(chan Task, channelBufferSize)
	errorChan := make(chan error, channelBufferSize)
	execTimeChan := make(chan time.Duration, channelBufferSize)
	workerPoolDone := make(chan struct{})

	tm := newTaskManager(taskChan, errorChan, execTimeChan, minWorkerCount, scaleInterval, workerPoolDone)

	return tm
}
