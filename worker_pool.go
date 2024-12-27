package taskman

import (
	"sync"
	"sync/atomic"

	"github.com/rs/zerolog/log"
)

// workerPool manages a pool of workers that execute tasks.
type workerPool struct {
	workersActive  atomic.Int32 // Number of active workers
	workersRunning atomic.Int32 // Number of running workers
	workersTotal   int          // Total number of workers in the pool

	// TODO: do a lookover for channel directions
	errorChan      chan<- error  // Send-only channel for errors
	stopChan       chan struct{} // Channel to signal stopping the worker pool
	taskChan       <-chan Task   // Receive-only channel for tasks
	workerPoolDone chan struct{} // Channel to signal worker pool is done

	wg sync.WaitGroup
}

// startWorker executes tasks from the task channel.
func (wp *workerPool) startWorker(id int) {
	log.Debug().Msgf("Starting worker %d", id)
	wp.workersRunning.Add(1)
	defer func() {
		wp.workersRunning.Add(-1)
		wp.wg.Done()
	}()

	for {
		select {
		case task, ok := <-wp.taskChan:
			if !ok {
				log.Debug().Msgf("Worker %d: task channel closed, exiting", id)
				return
			}
			log.Debug().Msgf("Worker %d executing task", id)
			wp.workersActive.Add(1) // Increment active workers
			err := task.Execute()
			if err != nil {
				// No retry policy is implemented, we just log the error for now
				log.Debug().Err(err).Msgf("Worker %d: task execution failed", id)
			}
			wp.errorChan <- err
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

// runningWorkers returns the number of running workers.
func (wp *workerPool) runningWorkers() int32 {
	return wp.workersRunning.Load()
}

// start starts the worker pool, creating workers according to wp.WorkerCount.
// TODO: consider shielding this method from multiple calls using a sync.Once
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
	close(wp.workerPoolDone) // Signal worker pool is done
}

func newWorkerPool(workersTotal int, errorChan chan error, taskChan chan Task, workerPoolDone chan struct{}) *workerPool {
	return &workerPool{
		errorChan:      errorChan,
		stopChan:       make(chan struct{}),
		taskChan:       taskChan,
		workerPoolDone: workerPoolDone,
		workersTotal:   workersTotal,
	}
}
