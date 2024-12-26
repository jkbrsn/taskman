package taskman

import (
	"container/heap"
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
)

var (
	// ErrDispatcherStopped is returned when a task is added to a stopped dispatcher.
	ErrDispatcherStopped = errors.New("dispatcher is stopped")
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

	ctx    context.Context    // Context for the dispatcher
	cancel context.CancelFunc // Cancel function for the dispatcher

	newTaskChan chan bool     // Channel to signal that new tasks have entered the queue
	resultChan  chan Result   // Channel to receive results from the worker pool
	runDone     chan struct{} // Channel to signal run has stopped
	taskChan    chan Task     // Channel to send tasks to the worker pool

	jobQueue priorityQueue // A priority queue to hold the scheduled jobs

	stopOnce sync.Once

	workerPool *workerPool
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
// Returns the resultings job's ID.
// Note: wraps the function in a BasicTask.
func (d *Dispatcher) AddFunc(function func() Result, cadence time.Duration) (string, error) {
	task := BasicTask{function}
	job := Job{
		Tasks:    []Task{task},
		Cadence:  cadence,
		ID:       strings.Split(uuid.New().String(), "-")[0],
		NextExec: time.Now().Add(cadence),
	}
	return d.AddJob(job)
}

// AddJob adds a job to the Dispatcher. A job is a group of tasks that are scheduled
// to execute together. The function returns a job ID that can be used to remove
// the job from the Dispatcher.
// Job requirements:
// - Cadence must be greater than 0
// - Job must have at least one task
// - NextExec must be non-zero
// - Job must have an ID, unique within the Dispatcher
func (d *Dispatcher) AddJob(job Job) (string, error) {
	// TODO: make the validation also check if the ID is already in use
	// Validate the job
	err := validateJob(job)
	if err != nil {
		return "", err
	}
	log.Debug().Msgf("Adding job with %d tasks with group ID '%s' and cadence %v", len(job.Tasks), job.ID, job.Cadence)

	// Check if the dispatcher is stopped
	select {
	case <-d.ctx.Done():
		// If the dispatcher is stopped, do not continue adding the job
		log.Debug().Msg("Dispatcher is stopped, not adding job")
		return "", ErrDispatcherStopped
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

	// TODO: return ID, or something else?
	return job.ID, nil
}

// AddTask takes a Task and adds it to the Dispatcher in a Job.
// Returns the resultings job's ID.
func (d *Dispatcher) AddTask(task Task, cadence time.Duration) (string, error) {
	job := Job{
		Tasks:    []Task{task},
		Cadence:  cadence,
		ID:       strings.Split(uuid.New().String(), "-")[0],
		NextExec: time.Now().Add(cadence),
	}
	return d.AddJob(job)
}

// AddTasks takes a slice of Task and adds them to the Dispatcher in a Job.
// Returns the resultings job's ID.
func (d *Dispatcher) AddTasks(tasks []Task, cadence time.Duration) (string, error) {
	// Generate a 12 char random ID as the job ID
	jobID := strings.Split(uuid.New().String(), "-")[0]
	log.Debug().Msgf("Adding job with %d tasks with group ID '%s' and cadence %v", len(tasks), jobID, cadence)

	// The job uses a copy of the tasks slice, to avoid unintended consequences if the original slice is modified
	job := &Job{
		Tasks:    append([]Task(nil), tasks...),
		Cadence:  cadence,
		ID:       jobID,
		NextExec: time.Now().Add(cadence),
	}
	return d.AddJob(*job)
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

// Results returns a read-only channel for consuming results.
func (d *Dispatcher) Results() <-chan Result {
	return d.resultChan
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
		// Note: resultChan is closed by workerPool.Stop()

		// Wait for the run loop to exit
		<-d.runDone

		// Close the remaining channels
		close(d.taskChan)
		close(d.newTaskChan)

		log.Debug().Msg("Dispatcher stopped")
	})
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
func validateJob(job Job) error {
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
	// TODO: revisit if immediate execution support is added
	if job.NextExec.IsZero() {
		return ErrZeroNextExec
	}
	return nil
}

// NewDispatcher creates, starts and returns a new Dispatcher.
func NewDispatcher(workerCount, taskBufferSize, resultBufferSize int) *Dispatcher {
	resultChan := make(chan Result, resultBufferSize)
	taskChan := make(chan Task, taskBufferSize)
	workerPool := &workerPool{
		resultChan:   resultChan,
		stopChan:     make(chan struct{}),
		taskChan:     taskChan,
		workersTotal: workerCount,
	}
	s := newDispatcher(workerPool, taskChan, resultChan)
	return s
}

// newDispatcher creates a new Dispatcher.
// The internal constructor pattern allows for dependency injection of internal components.
func newDispatcher(workerPool *workerPool, taskChan chan Task, resultChan chan Result) *Dispatcher {
	log.Debug().Msg("Creating new dispatcher")

	// Input validation
	if workerPool == nil {
		panic("workerPool cannot be nil")
	}
	if taskChan == nil {
		panic("taskChan cannot be nil")
	}
	if resultChan == nil {
		panic("resultChan cannot be nil")
	}

	ctx, cancel := context.WithCancel(context.Background())

	d := &Dispatcher{
		ctx:         ctx,
		cancel:      cancel,
		jobQueue:    make(priorityQueue, 0),
		newTaskChan: make(chan bool, 1),
		resultChan:  resultChan,
		runDone:     make(chan struct{}),
		taskChan:    taskChan,
		workerPool:  workerPool,
	}

	heap.Init(&d.jobQueue)

	log.Debug().Msg("Starting dispatcher")
	d.workerPool.start()
	go d.run()

	return d
}
