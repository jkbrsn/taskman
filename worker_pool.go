package taskman

import (
	"sync"
	"sync/atomic"

	"github.com/rs/xid"
	"github.com/rs/zerolog/log"
)

// workerPool manages a pool of workers that execute tasks.
type workerPool struct {
	workersActive  atomic.Int32 // Number of active workers
	workersRunning atomic.Int32 // Number of running workers

	errorChan      chan<- error  // Send-only channel for errors
	stopChan       chan struct{} // Channel to signal stopping the worker pool
	taskChan       <-chan Task   // Receive-only channel for tasks
	workerPoolDone chan struct{} // Channel to signal worker pool is done

	wg sync.WaitGroup
}

// activeWorkers returns the number of active workers.
func (wp *workerPool) activeWorkers() int32 {
	return wp.workersActive.Load()
}

// runningWorkers returns the number of running workers.
func (wp *workerPool) runningWorkers() int32 {
	return wp.workersRunning.Load()
}

// addWorkers adds to the worker pool by starting new workers.
func (wp *workerPool) addWorkers(nWorkers int) {
	log.Info().Msgf("Adding %d new workers to the pool", nWorkers)
	wp.wg.Add(nWorkers)
	for i := 0; i < nWorkers; i++ {
		workerID := xid.New().String()
		go wp.startWorker(workerID)
	}
}

// startWorker executes tasks from the task channel.
func (wp *workerPool) startWorker(id string) {
	log.Debug().Msgf("Starting worker %s", id)
	wp.workersRunning.Add(1)
	defer func() {
		wp.workersRunning.Add(-1)
		wp.wg.Done()
	}()

	for {
		select {
		case task, ok := <-wp.taskChan:
			if !ok {
				log.Debug().Msgf("Worker %s: task channel closed, exiting", id)
				return
			}
			log.Debug().Msgf("Worker %s executing task", id)
			wp.workersActive.Add(1) // Increment active workers
			err := task.Execute()
			if err != nil {
				// No retry policy is implemented, we just log and send the error for now
				log.Debug().Err(err).Msgf("Worker %s: task execution failed", id)
				wp.errorChan <- err
			}
			wp.workersActive.Add(-1) // Decrement active workers
			log.Debug().Msgf("Worker %s: finished task", id)
		case <-wp.stopChan:
			log.Debug().Msgf("Worker %s: received stop signal, exiting", id)
			return
		}
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

// newWorkerPool creates and returns a new worker pool.
func newWorkerPool(initialWorkers int, errorChan chan error, taskChan chan Task, workerPoolDone chan struct{}) *workerPool {
	pool := &workerPool{
		errorChan:      errorChan,
		stopChan:       make(chan struct{}),
		taskChan:       taskChan,
		workerPoolDone: workerPoolDone,
	}
	pool.addWorkers(initialWorkers)
	return pool
}
