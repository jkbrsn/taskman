package taskman

import (
	"container/heap"
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
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

	ctx      context.Context    // Context for the manager
	cancel   context.CancelFunc // Cancel function for the manager
	runDone  chan struct{}      // Channel to signal run has stopped
	stopOnce sync.Once          // Ensures Stop is only called once

	newJobChan chan bool // Channel to signal that new tasks have entered the queue
	taskChan   chan Task // Channel to send tasks to the worker pool

	jobQueue priorityQueue // A priority queue to hold the scheduled jobs

	errorChan   chan error  // Channel to receive errors from the worker pool
	externalErr atomic.Bool // Tracks ownership of error channel consumption

	workerPool     *workerPool
	workerPoolDone chan struct{} // Channel to receive signal that the worker pool has stopped
}

// Task is an interface for tasks that can be executed.
type Task interface {
	Execute() error
}

// BasicTask is a task that executes a function.
type BasicTask struct {
	Function func() error
}

// Execure executes the function and returns the error.
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

// ScheduleFunc takes a function and adds it to the Manager in a Job. Creates and returns a
// randomized ID, used to identify the Job within the manager.
// Note: wraps the function in a BasicTask.
func (d *Manager) ScheduleFunc(function func() error, cadence time.Duration) (string, error) {
	task := BasicTask{function}
	jobID := strings.Split(uuid.New().String(), "-")[0]

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
	jobID := strings.Split(uuid.New().String(), "-")[0]

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
	jobID := strings.Split(uuid.New().String(), "-")[0]

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

// ReplaceJob replaces a job in the Manager's queue with a new job, based on
// their ID:s matching. The new job's NextExec will be overwritten by the old
// job's, to preserve the Manager's schedule.
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

// ErrorChannel returns a read-only channel for consuming errors from task execution. Calling this
// transfers responsibility to consume errors to the caller, which is expected to keep doing so
// until the Manager has completely stopped. Not consuming errors may lead to a block in the
// worker pool.
func (d *Manager) ErrorChannel() (<-chan error, error) {
	if !d.externalErr.CompareAndSwap(false, true) {
		return nil, errors.New("ErrorChannel can only be called once, returning nil")

	}
	return d.errorChan, nil
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

// jobsInQueue returns the length of the jobQueue slice.
func (d *Manager) jobsInQueue() int {
	d.Lock()
	defer d.Unlock()

	return d.jobQueue.Len()
}

// run runs the Manager.
// This function is intended to be run as a goroutine.
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

// NewManager creates, starts and returns a new Manager.
func NewManager(workerCount, taskBufferSize, errorBufferSize int) *Manager {
	errorChan := make(chan error, errorBufferSize)
	taskChan := make(chan Task, taskBufferSize)
	workerPoolDone := make(chan struct{})
	workerPool := newWorkerPool(workerCount, errorChan, taskChan, workerPoolDone)
	s := newManager(workerPool, taskChan, errorChan, workerPoolDone)
	return s
}

// newManager creates a new Manager.
// The internal constructor pattern allows for dependency injection of internal components.
func newManager(workerPool *workerPool, taskChan chan Task, errorChan chan error, workerPoolDone chan struct{}) *Manager {
	log.Debug().Msg("Creating new manager")

	// Input validation
	if workerPool == nil {
		panic("workerPool cannot be nil")
	}
	if taskChan == nil {
		panic("taskChan cannot be nil")
	}
	if errorChan == nil {
		panic("errorChan cannot be nil")
	}

	ctx, cancel := context.WithCancel(context.Background())

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
	d.workerPool.start()
	go d.run()
	go d.consumeErrorChan()

	return d
}
