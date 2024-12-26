package taskman

import (
	"sync"
	"sync/atomic"

	"github.com/rs/zerolog/log"
)

// workerPool manages a pool of workers that execute tasks.
type workerPool struct {
	workersTotal  int          // Total number of workers in the pool
	workersActive atomic.Int32 // Number of active workers

	// TODO: do a lookover for channel directions
	resultChan chan<- Result // Send-only channel for results
	stopChan   chan struct{} // Channel to signal stopping the worker pool
	taskChan   <-chan Task   // Receive-only channel for tasks

	wg sync.WaitGroup
}

// startWorker executes tasks from the task channel.
func (wp *workerPool) startWorker(id int) {
	defer wp.wg.Done()

	for {
		select {
		case task, ok := <-wp.taskChan:
			if !ok {
				log.Debug().Msgf("Worker %d: task channel closed, exiting", id)
				return
			}
			log.Debug().Msgf("Worker %d executing task", id)
			wp.workersActive.Add(1) // Increment active workers
			// TODO: consider processing the task in a separate goroutine, e.g. async task execution within a single worker
			// TODO cont.: this would allow the worker to continue processing tasks while waiting for the result
			result := task.Execute()
			if result.Error != nil {
				// No retry policy is implemented, we just log the error for now
				// TODO: consider leaving all error handling to the caller, even logging
				log.Error().Err(result.Error).Msgf("Worker %d: task execution failed", id)
			}
			wp.resultChan <- result
			wp.workersActive.Add(-1) // Decrement active workers
			log.Debug().Msgf("Worker %d: finished task", id)
		case <-wp.stopChan:
			log.Debug().Msgf("Worker %d: received stop signal, exiting", id)
			return
		}
	}
}

// activeWorkers returns the number of active workers.
func (wp *workerPool) activeWorkers() int32 {
	return wp.workersActive.Load()
}

// start starts the worker pool, creating workers according to wp.WorkerCount.
func (wp *workerPool) start() {
	log.Info().Msgf("Starting worker pool with %d workers", wp.workersTotal)
	wp.wg.Add(wp.workersTotal)
	for i := 0; i < wp.workersTotal; i++ {
		go wp.startWorker(i)
	}
}

// stop signals the worker pool to stop processing tasks and exit.
func (wp *workerPool) stop() {
	log.Debug().Msg("Attempting worker pool stop")
	close(wp.stopChan) // Signal workers to stop
	log.Debug().Msg("Waiting for workers to finish")
	wp.wg.Wait() // Wait for all workers to finish
	log.Debug().Msg("Worker pool stopped")
	close(wp.resultChan)
}
