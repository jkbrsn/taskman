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

// Scheduler manages task scheduling, using a worker pool to execture tasks based on their cadence.
type Scheduler struct {
	sync.RWMutex

	ctx    context.Context    // Context for the scheduler
	cancel context.CancelFunc // Cancel function for the scheduler

	newTaskChan chan bool     // Channel to signal that new tasks have entered the queue
	resultChan  chan Result   // Channel to receive results from the worker pool
	runDone     chan struct{} // Channel to signal run has stopped
	taskChan    chan Task     // Channel to send tasks to the worker pool

	jobQueue PriorityQueue // A priority queue to hold the scheduled jobs

	stopOnce sync.Once

	workerPool *WorkerPool
}

// ScheduledJob represents a group of tasks that are scheduled for execution.
type ScheduledJob struct {
	Tasks []Task

	Cadence  time.Duration
	ID       string
	NextExec time.Time

	index int // Index within the heap
}

// AddFunc takes a function and adds it to the Scheduler as a Task.
func (s *Scheduler) AddFunc(function func() Result, cadence time.Duration) string {
	task := BasicTask{function}
	return s.AddJob([]Task{task}, cadence)
}

// AddTask adds a Task to the Scheduler.
// Note: wrapper to simplify adding single tasks.
func (s *Scheduler) AddTask(task Task, cadence time.Duration) string {
	return s.AddJob([]Task{task}, cadence)
}

/*
AddJob adds a job of N tasks to the Scheduler. A job is a group of tasks that
are scheduled to execute together. Tasks must implement the Task interface and
the input cadence must be greater than 0. The function returns a job ID that
can be used to remove the job from the Scheduler.
*/
func (s *Scheduler) AddJob(tasks []Task, cadence time.Duration) string {
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
	job := &ScheduledJob{
		Tasks:    append([]Task(nil), tasks...),
		Cadence:  cadence,
		ID:       jobID,
		NextExec: time.Now().Add(cadence),
	}

	// Check if the scheduler is stopped
	select {
	case <-s.ctx.Done():
		// If the scheduler is stopped, do not continue adding the job
		// TODO: return an error?
		log.Debug().Msg("Scheduler is stopped, not adding job")
		return ""
	default:
		// Do nothing if the scheduler isn't stopped
	}

	// Push the job to the queue
	s.Lock()
	heap.Push(&s.jobQueue, job)
	s.Unlock()

	// Signal the scheduler to check for new tasks
	select {
	case <-s.ctx.Done():
		// Do nothing if the scheduler is stopped
		log.Debug().Msg("Scheduler is stopped, not signaling new task")
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

// RemoveJob removes a job from the Scheduler.
func (s *Scheduler) RemoveJob(jobID string) {
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

// Start starts the Scheduler.
// With this design, the Scheduler manages its own goroutine internally.
func (s *Scheduler) Start() {
	log.Info().Msg("Starting scheduler")
	go s.run()
}

// run runs the Scheduler.
// This function is intended to be run as a goroutine.
func (s *Scheduler) run() {
	defer func() {
		log.Debug().Msg("Scheduler run loop exiting")
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
				log.Info().Msg("Scheduler received stop signal, exiting run loop")
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
						log.Info().Msg("Scheduler received stop signal during task dispatch, exiting run loop")
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
				log.Info().Msg("Scheduler received stop signal during wait, exiting run loop")
				return
			}
		}
	}
}

// Results returns a read-only channel for consuming results.
func (s *Scheduler) Results() <-chan Result {
	return s.resultChan
}

// Stop signals the Scheduler to stop processing tasks and exit.
// Note: blocks until the Scheduler, including all workers, has completely stopped.
func (s *Scheduler) Stop() {
	log.Debug().Msg("Attempting scheduler stop")
	s.stopOnce.Do(func() {
		// Signal the scheduler to stop
		s.cancel()

		// Stop the worker pool
		s.workerPool.Stop()
		// Note: resultChan is closed by workerPool.Stop()

		// Wait for the run loop to exit
		<-s.runDone

		// Close the remaining channels
		close(s.taskChan)
		close(s.newTaskChan)

		log.Debug().Msg("Scheduler stopped")
	})
}

// NewScheduler creates, starts and returns a new Scheduler.
func NewScheduler(workerCount, taskBufferSize, resultBufferSize int) *Scheduler {
	resultChan := make(chan Result, resultBufferSize)
	taskChan := make(chan Task, taskBufferSize)
	workerPool := NewWorkerPool(resultChan, taskChan, workerCount)
	s := newScheduler(workerPool, taskChan, resultChan)
	return s
}

// newScheduler creates a new Scheduler.
// The internal constructor pattern allows for dependency injection of internal components.
func newScheduler(workerPool *WorkerPool, taskChan chan Task, resultChan chan Result) *Scheduler {
	log.Debug().Msg("Creating new scheduler")

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

	s := &Scheduler{
		ctx:         ctx,
		cancel:      cancel,
		jobQueue:    make(PriorityQueue, 0),
		newTaskChan: make(chan bool, 1),
		resultChan:  resultChan,
		runDone:     make(chan struct{}),
		taskChan:    taskChan,
		workerPool:  workerPool,
	}

	heap.Init(&s.jobQueue)

	log.Debug().Msg("Starting scheduler")
	s.workerPool.Start()
	go s.run()

	return s
}
