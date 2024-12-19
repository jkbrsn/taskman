package taskman

import (
	"container/heap"
	"context"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
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
	Tasks []Task

	Cadence  time.Duration
	ID       string
	NextExec time.Time

	index int // Index within the heap
}

// AddFunc takes a function and adds it to the Dispatcher as a Task.
func (s *Dispatcher) AddFunc(function func() Result, cadence time.Duration) string {
	task := BasicTask{function}
	return s.AddJob([]Task{task}, cadence)
}

// AddTask adds a Task to the Dispatcher.
// Note: wrapper to simplify adding single tasks.
func (s *Dispatcher) AddTask(task Task, cadence time.Duration) string {
	return s.AddJob([]Task{task}, cadence)
}

/*
AddJob adds a job of N tasks to the Dispatcher. A job is a group of tasks that
are scheduled to execute together. Tasks must implement the Task interface and
the input cadence must be greater than 0. The function returns a job ID that
can be used to remove the job from the Dispatcher.
*/
func (s *Dispatcher) AddJob(tasks []Task, cadence time.Duration) string {
	// Jobs with cadence <= 0 are ignored, as such a job would execute immediately and continuously
	// and risk overwhelming the worker pool.
	if cadence <= 0 {
		// TODO: return an error?
		log.Warn().Msgf("Not adding job: cadence must be greater than 0 (was %v)", cadence)
		return ""
	}

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

	// Check if the dispatcher is stopped
	select {
	case <-s.ctx.Done():
		// If the dispatcher is stopped, do not continue adding the job
		// TODO: return an error?
		log.Debug().Msg("Dispatcher is stopped, not adding job")
		return ""
	default:
		// Do nothing if the dispatcher isn't stopped
	}

	// Push the job to the queue
	s.Lock()
	heap.Push(&s.jobQueue, job)
	s.Unlock()

	// Signal the dispatcher to check for new tasks
	select {
	case <-s.ctx.Done():
		// Do nothing if the dispatcher is stopped
		log.Debug().Msg("Dispatcher is stopped, not signaling new task")
	default:
		select {
		case s.newTaskChan <- true:
			log.Trace().Msg("Signaled new job added")
		default:
			// Do nothing if no one is listening
		}
	}
	return jobID
}

// RemoveJob removes a job from the Dispatcher.
func (s *Dispatcher) RemoveJob(jobID string) {
	s.Lock()
	defer s.Unlock()

	// Find the job in the heap and remove it
	for i, job := range s.jobQueue {
		if job.ID == jobID {
			log.Debug().Msgf("Removing job with ID '%s'", jobID)
			heap.Remove(&s.jobQueue, i)
			break
		}
	}
	log.Warn().Msgf("Job with ID '%s' not found, no job was removed", jobID)
}

// Start starts the Dispatcher.
// With this design, the Dispatcher manages its own goroutine internally.
func (s *Dispatcher) Start() {
	log.Info().Msg("Starting dispatcher")
	go s.run()
}

// run runs the Dispatcher.
// This function is intended to be run as a goroutine.
func (s *Dispatcher) run() {
	defer func() {
		log.Debug().Msg("Dispatcher run loop exiting")
		close(s.runDone)
	}()
	for {
		s.Lock()
		if s.jobQueue.Len() == 0 {
			s.Unlock()
			select {
			case <-s.newTaskChan:
				log.Trace().Msg("New task added, checking for next job")
				continue
			case <-s.ctx.Done():
				log.Info().Msg("Dispatcher received stop signal, exiting run loop")
				return
			}
		} else {
			nextJob := s.jobQueue[0]
			now := time.Now()
			delay := nextJob.NextExec.Sub(now)
			if delay <= 0 {
				log.Debug().Msgf("Executing job %s", nextJob.ID)
				heap.Pop(&s.jobQueue)
				s.Unlock()

				// Execute all tasks in the job
				for _, task := range nextJob.Tasks {
					select {
					case <-s.ctx.Done():
						log.Info().Msg("Dispatcher received stop signal during task dispatch, exiting run loop")
						return
					case s.taskChan <- task:
						// Successfully sent the task
					}
				}

				// Reschedule the job
				nextJob.NextExec = nextJob.NextExec.Add(nextJob.Cadence)
				s.Lock()
				heap.Push(&s.jobQueue, nextJob)
				s.Unlock()
				continue
			}
			s.Unlock()

			// Wait until the next job is due or until stopped.
			select {
			case <-time.After(delay):
				// Time to execute the next job
				continue
			case <-s.ctx.Done():
				log.Info().Msg("Dispatcher received stop signal during wait, exiting run loop")
				return
			}
		}
	}
}

// Results returns a read-only channel for consuming results.
func (s *Dispatcher) Results() <-chan Result {
	return s.resultChan
}

// Stop signals the Dispatcher to stop processing tasks and exit.
// Note: blocks until the Dispatcher, including all workers, has completely stopped.
func (s *Dispatcher) Stop() {
	log.Debug().Msg("Attempting dispatcher stop")
	s.stopOnce.Do(func() {
		// Signal the dispatcher to stop
		s.cancel()

		// Stop the worker pool
		s.workerPool.Stop()
		// Note: resultChan is closed by workerPool.Stop()

		// Wait for the run loop to exit
		<-s.runDone

		// Close the remaining channels
		close(s.taskChan)
		close(s.newTaskChan)

		log.Debug().Msg("Dispatcher stopped")
	})
}

// NewDispatcher creates, starts and returns a new Dispatcher.
func NewDispatcher(workerCount, taskBufferSize, resultBufferSize int) *Dispatcher {
	resultChan := make(chan Result, resultBufferSize)
	taskChan := make(chan Task, taskBufferSize)
	workerPool := NewworkerPool(resultChan, taskChan, workerCount)
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

	s := &Dispatcher{
		ctx:         ctx,
		cancel:      cancel,
		jobQueue:    make(priorityQueue, 0),
		newTaskChan: make(chan bool, 1),
		resultChan:  resultChan,
		runDone:     make(chan struct{}),
		taskChan:    taskChan,
		workerPool:  workerPool,
	}

	heap.Init(&s.jobQueue)

	log.Debug().Msg("Starting dispatcher")
	s.workerPool.Start()
	go s.run()

	return s
}
