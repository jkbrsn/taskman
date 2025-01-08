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
	ErrManagerStopped = errors.New("manager is stopped")
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

// Manager manages task scheduling and execution.
// It dispatches scheduled jobs to a worker pool for execution.
type Manager struct {
	sync.RWMutex

	// Queue
	jobQueue   priorityQueue // A priority queue to hold the scheduled jobs
	newJobChan chan bool     // Channel to signal that new tasks have entered the queue

	// Context and operations
	ctx      context.Context    // Context for the manager
	cancel   context.CancelFunc // Cancel function for the manager
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
// until the Manager has completely stopped. Not consuming errors may lead to a block in the
// worker pool.
// TODO: redesign default channel consumption
func (d *Manager) ErrorChannel() (<-chan error, error) {
	if !d.externalErr.CompareAndSwap(false, true) {
		return nil, errors.New("ErrorChannel can only be called once, returning nil")

	}
	return d.errorChan, nil
}

// ScheduleFunc takes a function and adds it to the Manager in a Job. Creates and returns a
// randomized ID, used to identify the Job within the manager.
// Note: wraps the function in a BasicTask.
func (d *Manager) ScheduleFunc(function func() error, cadence time.Duration) (string, error) {
	task := BasicTask{function}
	jobID := xid.New().String()

	job := Job{
		Tasks:    []Task{task},
		Cadence:  cadence,
		ID:       jobID,
		NextExec: time.Now().Add(cadence),
	}

	return jobID, d.ScheduleJob(job)
}

// ScheduleJob adds a job to the Manager. A job is a group of tasks that are scheduled to execute
// together. The function returns a job ID that can be used to identify the job within the Manager.
// Job requirements:
// - Cadence must be greater than 0
// - Job must have at least one task
// - NextExec must be non-zero and positive
// - Job must have an ID, unique within the Manager
func (d *Manager) ScheduleJob(job Job) error {
	d.Lock()
	defer d.Unlock()

	// Validate the job
	err := d.validateJob(job)
	if err != nil {
		return err
	}
	log.Debug().Msgf("Adding job with %d tasks with ID '%s' and cadence %v", len(job.Tasks), job.ID, job.Cadence)

	// Check if the manager is stopped
	select {
	case <-d.ctx.Done():
		// If the manager is stopped, do not continue adding the job
		log.Debug().Msg("Manager is stopped, not adding job")
		return ErrManagerStopped
	default:
		// Do nothing if the manager isn't stopped
	}

	// Update task metrics
	if len(job.Tasks) > int(d.metrWidestJob.Load()) {
		d.metrWidestJob.Store(int32(len(job.Tasks)))
	}
	metrTasksPerSecond := calcTasksPerSecond(len(job.Tasks), job.Cadence)
	d.updateTaskMetrics(len(job.Tasks), metrTasksPerSecond)

	// Scale worker pool if needed
	// TODO: test behavior of this
	d.scaleWorkerPool()

	// Push the job to the queue
	heap.Push(&d.jobQueue, &job)

	// Signal the manager to check for new tasks
	select {
	case <-d.ctx.Done():
		// Do nothing if the manager is stopped
		log.Debug().Msg("Manager is stopped, not signaling new task")
	default:
		select {
		case d.newJobChan <- true:
			log.Trace().Msg("Signaled new job added")
		default:
			// Do nothing if no one is listening
		}
	}

	return nil
}

// ScheduleTask takes a Task and adds it to the Manager in a Job. Creates and returns a randomized
// ID, used to identify the Job within the manager.
func (d *Manager) ScheduleTask(task Task, cadence time.Duration) (string, error) {
	jobID := xid.New().String()

	job := Job{
		Tasks:    append([]Task(nil), []Task{task}...),
		Cadence:  cadence,
		ID:       jobID,
		NextExec: time.Now().Add(cadence),
	}

	return jobID, d.ScheduleJob(job)
}

// ScheduleTasks takes a slice of Task and adds them to the Manager in a Job. Creates and returns a
// randomized ID, used to identify the Job within the manager.
func (d *Manager) ScheduleTasks(tasks []Task, cadence time.Duration) (string, error) {
	jobID := xid.New().String()

	// Takes a copy of the tasks, avoiding unintended consequences if the slice is modified
	job := Job{
		Tasks:    append([]Task(nil), tasks...),
		Cadence:  cadence,
		ID:       jobID,
		NextExec: time.Now().Add(cadence),
	}

	return jobID, d.ScheduleJob(job)
}

// RemoveJob removes a job from the Manager.
func (d *Manager) RemoveJob(jobID string) error {
	d.Lock()
	defer d.Unlock()

	return d.jobQueue.RemoveByID(jobID)
}

// ReplaceJob replaces a job in the Manager's queue with a new job, based on their ID:s matching.
// The new job's NextExec will be overwritten by the old job's, to preserve the Manager's schedule.
func (d *Manager) ReplaceJob(newJob Job) error {
	d.Lock()
	defer d.Unlock()

	jobIndex, err := d.jobQueue.JobInQueue(newJob.ID)
	if err != nil {
		return ErrJobNotFound
	}
	log.Debug().Msgf("Replacing job with ID: %s", newJob.ID)
	oldJob := d.jobQueue[jobIndex]
	newJob.NextExec = oldJob.NextExec
	newJob.index = oldJob.index
	d.jobQueue[jobIndex] = &newJob
	return nil
}

// Stop signals the Manager to stop processing tasks and exit.
// Note: blocks until the Manager, including all workers, has completely stopped.
func (d *Manager) Stop() {
	log.Debug().Msg("Attempting manager stop")
	d.stopOnce.Do(func() {
		// Signal the manager to stop
		d.cancel()

		// Stop the worker pool
		d.workerPool.stop()

		// Wait for the run loop to exit, and the worker pool to stop
		<-d.runDone
		<-d.workerPoolDone

		// Close the remaining channels
		close(d.newJobChan)
		close(d.errorChan)
		close(d.taskChan)

		log.Debug().Msg("Manager stopped")
	})
}

// updateTaskMetrics updates the task metrics.
func (d *Manager) updateTaskMetrics(additionalTasks int, metrTasksPerSecond float32) {
	// Update the tasks per second metric base on a weighted average
	newmetrTasksPerSecond := (metrTasksPerSecond*float32(additionalTasks) + d.metrTasksPerSecond.Load()*float32(d.metrTotalTaskCount.Load())) / float32(d.metrTotalTaskCount.Load()+int64(additionalTasks))

	// Store updated values
	d.metrTasksPerSecond.Store(newmetrTasksPerSecond)
	d.metrTotalTaskCount.Add(int64(additionalTasks))
}

// consumeErrorChan handles errors until ErrorChannel() is called.
func (d *Manager) consumeErrorChan() {
	for {
		select {
		case err := <-d.errorChan:
			if d.externalErr.Load() {
				log.Debug().Err(err).Msg("External caller taking control of error consumption")
				// Resend the error to the external listener
				go func(e error) {
					d.errorChan <- e
				}(err)
				// Close the internal consumer
				return
			}
			// Handle error internally (e.g., log it)
			log.Debug().Err(err).Msg("Unhandled error")
		case <-d.workerPoolDone:
			// Only stop consuming once the worker pool is done, since any
			// remaining workers will need to send errors on errorChan
			return
		}
	}
}

// consumeMetrics consumes execution times from the worker pool, and uses the data to calculate the
// average execution time of tasks.
func (d *Manager) consumeMetrics() {
	execTimeChan, err := d.workerPool.execTimeChannel()
	if err != nil {
		log.Error().Err(err).Msg("Failed to get exec time channel")
		return
	}

	for {
		select {
		case execTime := <-execTimeChan:
			log.Debug().Msgf("Exec time received: %v", execTime)
			avgExecTime := d.metrAvgExecTime.Load()
			taskCount := d.metrTaskCount.Load()

			// Calculate the new average execution time
			newAvgExecTime := (avgExecTime*time.Duration(taskCount) + execTime) / time.Duration(taskCount+1)

			// Store the updated metrics
			d.metrAvgExecTime.Store(newAvgExecTime)
			d.metrTaskCount.Add(1)
		case <-d.workerPoolDone:
			// Only stop consuming once the worker pool is done, since any
			// remaining workers will need to send exec times before closing
			return
		}

	}

}

// jobsInQueue returns the length of the jobQueue slice.
func (d *Manager) jobsInQueue() int {
	d.Lock()
	defer d.Unlock()

	return d.jobQueue.Len()
}

// run runs the Manager.
func (d *Manager) run() {
	defer func() {
		log.Debug().Msg("Manager run loop exiting")
		close(d.runDone)
	}()
	for {
		d.Lock()
		if d.jobQueue.Len() == 0 {
			d.Unlock()
			select {
			case <-d.newJobChan:
				log.Trace().Msg("New job added, checking for next job")
				continue
			case <-d.ctx.Done():
				log.Info().Msg("Manager received stop signal, exiting run loop")
				return
			}
		} else {
			nextJob := d.jobQueue[0]
			now := time.Now()
			delay := nextJob.NextExec.Sub(now)
			if delay <= 0 {
				log.Debug().Msgf("Dispatching job %s", nextJob.ID)
				heap.Pop(&d.jobQueue)
				d.Unlock()

				// Dispatch all tasks in the job to the worker pool for execution
				for _, task := range nextJob.Tasks {
					select {
					case <-d.ctx.Done():
						log.Info().Msg("Manager received stop signal during task dispatch, exiting run loop")
						return
					case d.taskChan <- task:
						// Successfully sent the task
					}
				}

				// Reschedule the job
				nextJob.NextExec = nextJob.NextExec.Add(nextJob.Cadence)
				d.Lock()
				heap.Push(&d.jobQueue, nextJob)
				d.Unlock()
				continue
			}
			d.Unlock()

			// Wait until the next job is due or until stopped.
			select {
			// TODO: also listen to newJobChan here?
			case <-time.After(delay):
				// Time to execute the next job
				continue
			case <-d.ctx.Done():
				log.Info().Msg("Manager received stop signal during wait, exiting run loop")
				return
			}
		}
	}
}

// scaleWorkerPool scales the worker pool based on current job queue stats.
func (d *Manager) scaleWorkerPool() {
	log.Debug().Msg("Scaling worker pool")
	currentWorkerCount := d.workerPool.runningWorkers()

	// Calculate the number of workers needed based on the widest job
	workersNeededParallelTasks := d.metrWidestJob.Load()

	// Calculate the number of workers needed based on the average execution time and tasks per second
	avgExecTimeSeconds := d.metrAvgExecTime.Load().Seconds()
	tasksPerSecond := float64(d.metrTasksPerSecond.Load())
	workersNeededConcurrently := int32(math.Ceil(avgExecTimeSeconds * tasksPerSecond))

	// TODO: consider adding a direct check, comparing current available workers to the immediate
	//       need based on the latest added job. This would allow for immediate scaling if needed.

	// Use the higher of the two metrics
	workersNeeded := workersNeededParallelTasks
	if workersNeededConcurrently > workersNeeded {
		workersNeeded = workersNeededConcurrently
	}

	// Add a buffer of extra workers on top of the calculated number
	// TODO: evaluate size of buffer factor
	bufferFactor := 1.5 // 50% buffer
	workersNeeded = int32(math.Ceil(float64(workersNeeded) * bufferFactor))

	// Adjust the worker pool size
	if workersNeeded > currentWorkerCount {
		log.Debug().Msgf("Scaling worker pool UP from %d to %d workers", currentWorkerCount, workersNeeded)
		// Scale up
		d.workerPool.addWorkers(int(workersNeeded - currentWorkerCount))
	} else if workersNeeded < currentWorkerCount {
		// Scale down cautiously
		utilizationThreshold := 0.4 // If above 40% utilization, do not scale down
		if d.workerPool.utilization() < utilizationThreshold {
			log.Debug().Msgf("Scaling worker pool DOWN from %d to %d workers", currentWorkerCount, workersNeeded)
			d.workerPool.stopWorkers(int(currentWorkerCount - workersNeeded))
		}
	}

}

// validateJob validates a Job.
// Note: does not acquire a mutex lock for accessing the jobQueue, that is up to the caller.
func (d *Manager) validateJob(job Job) error {
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
	// Job ID:s are unique, so duplicates are not allowed to be added.
	if _, ok := d.jobQueue.JobInQueue(job.ID); ok == nil {
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

// newManager creates, initializes, and starts a new Manager.
func newManager(
	taskChan chan Task,
	errorChan chan error,
	execTimeChan chan time.Duration,
	initWorkerCount int,
	workerPoolDone chan struct{},
) *Manager {
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

	d := &Manager{
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

	heap.Init(&d.jobQueue)

	log.Debug().Msg("Starting manager")
	go d.run()
	go d.consumeErrorChan()
	go d.consumeMetrics()

	return d
}

// NewManager creates, starts and returns a new Manager.
func NewManager() *Manager {
	channelBufferSize := 256
	taskChan := make(chan Task, channelBufferSize)
	errorChan := make(chan error, channelBufferSize)
	execTimeChan := make(chan time.Duration, channelBufferSize)
	initWorkerCount := 32
	workerPoolDone := make(chan struct{})

	s := newManager(taskChan, errorChan, execTimeChan, initWorkerCount, workerPoolDone)

	return s
}

// NewManagerCustom creates, starts and returns a new Manager using custom values for some of the
// task manager parameters.
func NewManagerCustom(initWorkerCount, channelBufferSize int) *Manager {
	taskChan := make(chan Task, channelBufferSize)
	errorChan := make(chan error, channelBufferSize)
	execTimeChan := make(chan time.Duration, channelBufferSize)
	workerPoolDone := make(chan struct{})
	s := newManager(taskChan, errorChan, execTimeChan, initWorkerCount, workerPoolDone)
	return s
}
