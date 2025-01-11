package taskman

import (
	"container/heap"
	"context"
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/xid"
	"github.com/rs/zerolog/log"

	uatomic "go.uber.org/atomic"
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
	runDone  chan struct{}      // Channel to signal run has stopped
	stopOnce sync.Once          // Ensures Stop is only called once

	// Shared with worker pool
	taskChan       chan Task   // Channel to send tasks to the worker pool
	errorChan      chan error  // Channel to receive errors from the worker pool
	externalErr    atomic.Bool // Tracks ownership of error channel consumption
	workerPool     *workerPool
	workerPoolDone chan struct{} // Channel to receive signal that the worker pool has stopped

	// Metrics
	// TODO: move to external metrics component?
	metrAvgExecTime    uatomic.Duration // Average execution time of tasks
	metrTaskCount      atomic.Int64     // Total number of tasks executed
	metrTasksPerSecond uatomic.Float32  // Number of tasks executed per second
	metrTotalTaskCount atomic.Int64     // Total number of tasks in the queue
	metrWidestJob      atomic.Int32     // Widest job in the queue in terms of number of tasks
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

// ErrorChannel returns a read-only channel for consuming errors from task execution. Calling this
// transfers responsibility to consume errors to the caller, which is expected to keep doing so
// until the TaskManager has completely stopped. Not consuming errors may lead to a block in the
// worker pool.
// TODO: redesign default channel consumption
func (tm *TaskManager) ErrorChannel() (<-chan error, error) {
	if !tm.externalErr.CompareAndSwap(false, true) {
		return nil, errors.New("ErrorChannel can only be called once, returning nil")

	}
	return tm.errorChan, nil
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
	if taskCount > int(tm.metrWidestJob.Load()) {
		tm.metrWidestJob.Store(int32(taskCount))
	}
	metrTasksPerSecond := calcTasksPerSecond(taskCount, job.Cadence)
	tm.updateTaskMetrics(taskCount, metrTasksPerSecond)

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

	return tm.jobQueue.RemoveByID(jobID)
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

// updateTaskMetrics updates the task metrics.
func (tm *TaskManager) updateTaskMetrics(additionalTasks int, metrTasksPerSecond float32) {
	// Update the tasks per second metric base on a weighted average
	newmetrTasksPerSecond := (metrTasksPerSecond*float32(additionalTasks) + tm.metrTasksPerSecond.Load()*float32(tm.metrTotalTaskCount.Load())) / float32(tm.metrTotalTaskCount.Load()+int64(additionalTasks))

	// Store updated values
	tm.metrTasksPerSecond.Store(newmetrTasksPerSecond)
	tm.metrTotalTaskCount.Add(int64(additionalTasks))
}

// consumeErrorChan handles errors until ErrorChannel() is called.
func (tm *TaskManager) consumeErrorChan() {
	for {
		select {
		case err := <-tm.errorChan:
			if tm.externalErr.Load() {
				log.Debug().Err(err).Msg("External caller taking control of error consumption")
				// Resend the error to the external listener
				go func(e error) {
					tm.errorChan <- e
				}(err)
				// Close the internal consumer
				return
			}
			// Handle error internally (e.g., log it)
			log.Debug().Err(err).Msg("Unhandled error")
		case <-tm.workerPoolDone:
			// Only stop consuming once the worker pool is done, since any
			// remaining workers will need to send errors on errorChan
			return
		}
	}
}

// consumeMetrics consumes execution times from the worker pool, and uses the data to calculate the
// average execution time of tasks.
func (tm *TaskManager) consumeMetrics() {
	log.Debug().Msg("Started consuming metrics")
	execTimeChan, err := tm.workerPool.execTimeChannel()
	if err != nil {
		log.Error().Err(err).Msg("Failed to get exec time channel")
		return
	}

	for {
		select {
		case execTime := <-execTimeChan:
			log.Debug().Msgf("Exec time received: %v", execTime)
			avgExecTime := tm.metrAvgExecTime.Load()
			taskCount := tm.metrTaskCount.Load()

			// Calculate the new average execution time
			newAvgExecTime := (avgExecTime*time.Duration(taskCount) + execTime) / time.Duration(taskCount+1)

			// Store the updated metrics
			tm.metrAvgExecTime.Store(newAvgExecTime)
			tm.metrTaskCount.Add(1)
		case <-tm.workerPoolDone:
			// Only stop consuming once the worker pool is done, since any
			// remaining workers will need to send exec times before closing
			return
		}

	}

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
				heap.Pop(&tm.jobQueue)
				tm.Unlock()

				// Dispatch all tasks in the job to the worker pool for execution
				for _, task := range nextJob.Tasks {
					select {
					case <-tm.ctx.Done():
						log.Info().Msg("TaskManager received stop signal during task dispatch, exiting run loop")
						return
					case tm.taskChan <- task:
						// Successfully sent the task
					}
				}

				// Reschedule the job
				nextJob.NextExec = nextJob.NextExec.Add(nextJob.Cadence)
				tm.Lock()
				heap.Push(&tm.jobQueue, nextJob)
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

// scaleWorkerPool scales the worker pool based on the current job queue.
// The worker pool is scaled based on the highest of three metrics:
// - The widest job in the queue in terms of number of tasks
// - The average execution time and concurrency of tasks
// - The number of tasks in the latest job related to available workers at the moment
func (tm *TaskManager) scaleWorkerPool(tasksInLatestJob int) {
	log.Debug().Msg("Scaling worker pool")
	bufferFactor50 := 1.5
	bufferFactor100 := 2.0

	// Calculate the number of workers needed based on the widest job
	workersNeededParallelTasks := tm.metrWidestJob.Load()
	// Apply 100% buffer for parallel tasks, as this is a low predictability metric
	workersNeededParallelTasks = int32(math.Ceil(float64(workersNeededParallelTasks) * bufferFactor100))
	log.Debug().Msgf("Job width worker need: %d", workersNeededParallelTasks)

	// Calculate the number of workers needed based on the average execution time and tasks/s
	avgExecTimeSeconds := tm.metrAvgExecTime.Load().Seconds()
	tasksPerSecond := float64(tm.metrTasksPerSecond.Load())
	log.Debug().Msgf("Avg exec time: %v, tasks/s: %f", avgExecTimeSeconds, tasksPerSecond)
	workersNeededConcurrently := int32(math.Ceil(avgExecTimeSeconds * tasksPerSecond))
	// Apply 50% buffer for concurrent tasks, as this is a more predictable metric
	workersNeededConcurrently = int32(math.Ceil(float64(workersNeededConcurrently) * bufferFactor50))
	log.Debug().Msgf("Concurrent worker need: %d", workersNeededConcurrently)

	// Calculate the number of workers needed based on the number of tasks in the latest job
	var workersNeededImmediately int32
	if tm.workerPool.availableWorkers() < int32(tasksInLatestJob) {
		log.Debug().Msgf("Available/running workers: %d/%d", tm.workerPool.availableWorkers(), tm.workerPool.runningWorkers())
		log.Debug().Msgf("Tasks in latest job: %d", tasksInLatestJob)
		// If there are not enough workers to handle the latest job, scale up immediately
		extraWorkersNeeded := int32(tasksInLatestJob) - tm.workerPool.availableWorkers()
		// Apply 50% buffer for immediate tasks, as this is a more predictable metric
		workersNeededImmediately = int32(math.Ceil(float64(tm.workerPool.runningWorkers()+extraWorkersNeeded) * bufferFactor50))
	}
	log.Debug().Msgf("Immediate worker need: %d", workersNeededImmediately)

	// Use the highest of the three metrics
	workersNeeded := workersNeededParallelTasks
	if workersNeededConcurrently > workersNeeded {
		workersNeeded = workersNeededConcurrently
	}
	if workersNeededImmediately > workersNeeded {
		workersNeeded = workersNeededImmediately
	}

	// Adjust the worker pool size
	scalingRequestChan := tm.workerPool.workerCountScalingChannel()
	scalingRequestChan <- workersNeeded
	log.Debug().Msgf("Worker pool scaling request sent: %d", workersNeeded)
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

// calcTasksPerSecond calculates the number of tasks executed per second.
func calcTasksPerSecond(nTasks int, cadence time.Duration) float32 {
	if cadence == 0 {
		return 0
	}
	return float32(nTasks) / float32(cadence.Seconds())
}

// newTaskManager creates, initializes, and starts a new TaskManager.
func newTaskManager(
	taskChan chan Task,
	errorChan chan error,
	execTimeChan chan time.Duration,
	initWorkerCount int,
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
	if initWorkerCount <= 0 {
		panic("initWorkerCount must be greater than 0")
	}
	if workerPoolDone == nil {
		panic("workerPoolDone cannot be nil")
	}

	ctx, cancel := context.WithCancel(context.Background())

	workerPool := newWorkerPool(initWorkerCount, errorChan, execTimeChan, taskChan, workerPoolDone)

	tm := &TaskManager{
		ctx:            ctx,
		cancel:         cancel,
		jobQueue:       make(priorityQueue, 0),
		newJobChan:     make(chan bool, 1),
		errorChan:      errorChan,
		runDone:        make(chan struct{}),
		taskChan:       taskChan,
		workerPool:     workerPool,
		workerPoolDone: workerPoolDone,
	}

	heap.Init(&tm.jobQueue)

	log.Debug().Msg("Starting TaskManager")
	go tm.run()
	go tm.consumeErrorChan()
	go tm.consumeMetrics()

	return tm
}

// New creates, starts and returns a new TaskManager.
func New() *TaskManager {
	channelBufferSize := 256
	taskChan := make(chan Task, channelBufferSize)
	errorChan := make(chan error, channelBufferSize)
	execTimeChan := make(chan time.Duration, channelBufferSize)
	initialWorkerCount := 4
	workerPoolDone := make(chan struct{})

	tm := newTaskManager(taskChan, errorChan, execTimeChan, initialWorkerCount, workerPoolDone)

	return tm
}

// newTaskManagerCustom creates, starts and returns a new TaskManager using custom values for some of
// the task manager parameters.
func newTaskManagerCustom(initWorkerCount, channelBufferSize int) *TaskManager {
	taskChan := make(chan Task, channelBufferSize)
	errorChan := make(chan error, channelBufferSize)
	execTimeChan := make(chan time.Duration, channelBufferSize)
	workerPoolDone := make(chan struct{})
	tm := newTaskManager(taskChan, errorChan, execTimeChan, initWorkerCount, workerPoolDone)
	return tm
}
