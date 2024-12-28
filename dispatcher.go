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
	// ErrDispatcherStopped is returned when a task is added to a stopped dispatcher.
	ErrDispatcherStopped = errors.New("dispatcher is stopped")
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

// Dispatcher manages task scheduling and execution.
// It dispatches scheduled jobs to a worker pool for execution.
type Dispatcher struct {
	sync.RWMutex

	ctx      context.Context    // Context for the dispatcher
	cancel   context.CancelFunc // Cancel function for the dispatcher
	runDone  chan struct{}      // Channel to signal run has stopped
	stopOnce sync.Once          // Ensures Stop is only called once

	newTaskChan chan bool // Channel to signal that new tasks have entered the queue
	taskChan    chan Task // Channel to send tasks to the worker pool

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

// Execure executes the function and returns the eventual error.
func (f BasicTask) Execute() error {
	err := f.Function()
	return err
}

// Job describes when to execute a specific group of tasks.
type Job struct {
	Cadence time.Duration
	Tasks   []Task

	ID       string
	NextExec time.Time

	index int // Index within the heap
}

// AddFunc takes a function and adds it to the Dispatcher in a Job.
// Creates and returns a randomized ID, used for the Job.
// Note: wraps the function in a BasicTask.
func (d *Dispatcher) AddFunc(function func() error, cadence time.Duration) (string, error) {
	task := BasicTask{function}
	jobID := strings.Split(uuid.New().String(), "-")[0]
	job := Job{
		Tasks:    []Task{task},
		Cadence:  cadence,
		ID:       jobID,
		NextExec: time.Now().Add(cadence),
	}
	return jobID, d.AddJob(job)
}

// AddJob adds a job to the Dispatcher. A job is a group of tasks that are scheduled
// to execute together. The function returns a job ID that can be used to remove
// the job from the Dispatcher.
// Job requirements:
// - Cadence must be greater than 0
// - Job must have at least one task
// - NextExec must be non-zero
// - Job must have an ID, unique within the Dispatcher
func (d *Dispatcher) AddJob(job Job) error {
	// Validate the job
	err := d.validateJob(job)
	if err != nil {
		return err
	}
	log.Debug().Msgf("Adding job with %d tasks with group ID '%s' and cadence %v", len(job.Tasks), job.ID, job.Cadence)

	// Check if the dispatcher is stopped
	select {
	case <-d.ctx.Done():
		// If the dispatcher is stopped, do not continue adding the job
		log.Debug().Msg("Dispatcher is stopped, not adding job")
		return ErrDispatcherStopped
	default:
		// Do nothing if the dispatcher isn't stopped
	}

	// Push the job to the queue
	d.Lock()
	heap.Push(&d.jobQueue, &job)
	d.Unlock()

	// Signal the dispatcher to check for new tasks
	select {
	case <-d.ctx.Done():
		// Do nothing if the dispatcher is stopped
		log.Debug().Msg("Dispatcher is stopped, not signaling new task")
	default:
		select {
		case d.newTaskChan <- true:
			log.Trace().Msg("Signaled new job added")
		default:
			// Do nothing if no one is listening
		}
	}

	return nil
}

// AddTask takes a Task and adds it to the Dispatcher in a Job.
// Creates and returns a randomized ID, used for the Job.
func (d *Dispatcher) AddTask(task Task, cadence time.Duration) (string, error) {
	jobID := strings.Split(uuid.New().String(), "-")[0]
	job := Job{
		Tasks:    []Task{task},
		Cadence:  cadence,
		ID:       jobID,
		NextExec: time.Now().Add(cadence),
	}
	return jobID, d.AddJob(job)
}

// AddTasks takes a slice of Task and adds them to the Dispatcher in a Job.
// Creates and returns a randomized ID, used for the Job.
func (d *Dispatcher) AddTasks(tasks []Task, cadence time.Duration) (string, error) {
	jobID := strings.Split(uuid.New().String(), "-")[0]
	log.Debug().Msgf("Adding job with %d tasks with group ID '%s' and cadence %v", len(tasks), jobID, cadence)

	// The job uses a copy of the tasks slice, to avoid unintended consequences if the original slice is modified
	job := &Job{
		Tasks:    append([]Task(nil), tasks...),
		Cadence:  cadence,
		ID:       jobID,
		NextExec: time.Now().Add(cadence),
	}
	return jobID, d.AddJob(*job)
}

// RemoveJob removes a job from the Dispatcher.
func (d *Dispatcher) RemoveJob(jobID string) error {
	d.Lock()
	defer d.Unlock()

	return d.jobQueue.RemoveByID(jobID)
}

// ReplaceJob replaces a job in the Dispatcher's queue with a new job, based on
// their ID:s matching. The new job's NextExec will be overwritten by the old
// job's, to preserve the Dispatcher's schedule.
func (d *Dispatcher) ReplaceJob(newJob Job) error {
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

// ErrorChannel returns a read-only channel for consuming errors from task execution.
// Calling this transfers responsibility for consuming errors to the caller.
func (d *Dispatcher) ErrorChannel() (<-chan error, error) {
	if !d.externalErr.CompareAndSwap(false, true) {
		return nil, errors.New("ErrorChannel can only be called once, returning nil")

	}
	return d.errorChan, nil
}

// Stop signals the Dispatcher to stop processing tasks and exit.
// Note: blocks until the Dispatcher, including all workers, has completely stopped.
func (d *Dispatcher) Stop() {
	log.Debug().Msg("Attempting dispatcher stop")
	d.stopOnce.Do(func() {
		// Signal the dispatcher to stop
		d.cancel()

		// Stop the worker pool
		d.workerPool.stop()

		// Wait for the run loop to exit, and the worker pool to stop
		<-d.runDone
		<-d.workerPoolDone

		// Close the remaining channels
		close(d.newTaskChan)
		close(d.errorChan)
		close(d.taskChan)

		log.Debug().Msg("Dispatcher stopped")
	})
}

// consumeErrorChan handles errors until ErrorChannel() is called.
func (d *Dispatcher) consumeErrorChan() {
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
func (d *Dispatcher) jobsInQueue() int {
	d.Lock()
	defer d.Unlock()

	return d.jobQueue.Len()
}

// run runs the Dispatcher.
// This function is intended to be run as a goroutine.
func (d *Dispatcher) run() {
	defer func() {
		log.Debug().Msg("Dispatcher run loop exiting")
		close(d.runDone)
	}()
	for {
		d.Lock()
		if d.jobQueue.Len() == 0 {
			d.Unlock()
			select {
			case <-d.newTaskChan:
				log.Trace().Msg("New task added, checking for next job")
				continue
			case <-d.ctx.Done():
				log.Info().Msg("Dispatcher received stop signal, exiting run loop")
				return
			}
		} else {
			nextJob := d.jobQueue[0]
			now := time.Now()
			delay := nextJob.NextExec.Sub(now)
			if delay <= 0 {
				log.Debug().Msgf("Executing job %s", nextJob.ID)
				heap.Pop(&d.jobQueue)
				d.Unlock()

				// Execute all tasks in the job
				for _, task := range nextJob.Tasks {
					select {
					case <-d.ctx.Done():
						log.Info().Msg("Dispatcher received stop signal during task dispatch, exiting run loop")
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
				log.Info().Msg("Dispatcher received stop signal during wait, exiting run loop")
				return
			}
		}
	}
}

// validateJob validates a Job.
func (d *Dispatcher) validateJob(job Job) error {
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
	d.Lock()
	defer d.Unlock()
	if _, ok := d.jobQueue.JobInQueue(job.ID); ok == nil {
		return ErrDuplicateJobID
	}
	return nil
}

// NewDispatcher creates, starts and returns a new Dispatcher.
func NewDispatcher(workerCount, taskBufferSize, errorBufferSize int) *Dispatcher {
	errorChan := make(chan error, errorBufferSize)
	taskChan := make(chan Task, taskBufferSize)
	workerPoolDone := make(chan struct{})
	workerPool := newWorkerPool(workerCount, errorChan, taskChan, workerPoolDone)
	s := newDispatcher(workerPool, taskChan, errorChan, workerPoolDone)
	return s
}

// newDispatcher creates a new Dispatcher.
// The internal constructor pattern allows for dependency injection of internal components.
func newDispatcher(workerPool *workerPool, taskChan chan Task, errorChan chan error, workerPoolDone chan struct{}) *Dispatcher {
	log.Debug().Msg("Creating new dispatcher")

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

	d := &Dispatcher{
		ctx:            ctx,
		cancel:         cancel,
		jobQueue:       make(priorityQueue, 0),
		newTaskChan:    make(chan bool, 1),
		errorChan:      errorChan,
		runDone:        make(chan struct{}),
		taskChan:       taskChan,
		workerPool:     workerPool,
		workerPoolDone: workerPoolDone,
	}

	heap.Init(&d.jobQueue)

	log.Debug().Msg("Starting dispatcher")
	d.workerPool.start()
	go d.run()
	go d.consumeErrorChan()

	return d
}
