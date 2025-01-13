package taskman

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/xid"
	"github.com/rs/zerolog/log"
)

// workerPool manages a pool of workers that execute tasks.
type workerPool struct {
	workers           sync.Map     // Map worker ID (xid.ID) to worker (workerInfo)
	workersActive     atomic.Int32 // Number of active workers
	workersRunning    atomic.Int32 // Number of running workers
	workerCountTarget atomic.Int32 // Target number of workers

	errorChan       chan<- error       // Send-only channel for errors
	execTimeChan    chan time.Duration // Channel to send execution times
	taskChan        <-chan Task        // Receive-only channel for tasks
	workerCountChan chan int32         // Channel to receive worker count changes
	stopPoolChan    chan struct{}      // Channel to signal stopping the worker pool
	workerPoolDone  chan struct{}      // Channel to signal worker pool is done

	mu sync.Mutex
	wg sync.WaitGroup
}

// worker represents a worker that executes tasks.
type workerInfo struct {
	id       xid.ID        // The worker ID
	busy     atomic.Bool   // True if worker is busy
	stopChan chan struct{} // Channel to signal stopping the worker
}

// activeWorkers returns the number of active workers.
func (wp *workerPool) activeWorkers() int32 {
	return wp.workersActive.Load()
}

// runningWorkers returns the number of running workers.
func (wp *workerPool) runningWorkers() int32 {
	return wp.workersRunning.Load()
}

// targetWorkerCount returns the pool's target worker count.
func (wp *workerPool) targetWorkerCount() int32 {
	return wp.workerCountTarget.Load()
}

func (wp *workerPool) availableWorkers() int32 {
	return wp.runningWorkers() - wp.activeWorkers()
}

// addWorkers adds to the worker pool by starting new workers.
func (wp *workerPool) addWorkers(nWorkers int) {
	log.Info().Msgf("Adding %d new workers to the pool", nWorkers)
	wp.wg.Add(nWorkers)
	for i := 0; i < nWorkers; i++ {
		workerID := xid.New()
		go wp.startWorker(workerID)
	}
}

// adjustWorkerCount adjusts the number of workers in the pool to match the target worker count.
func (pool *workerPool) adjustWorkerCount(newTargetCount int32) {
	currentTarget := pool.targetWorkerCount()

	// Add or remove workers as needed
	if newTargetCount > currentTarget {
		log.Info().Msgf("Scaling worker pool UP from %d to %d workers", currentTarget, newTargetCount)
		// Scale up
		pool.addWorkers(int(newTargetCount - currentTarget))
	} else if newTargetCount < currentTarget {
		// Scale down cautiously
		utilizationThreshold := 0.4 // If above 40% utilization, do not scale down
		if pool.utilization() < utilizationThreshold {
			log.Info().Msgf("Scaling worker pool DOWN from %d to %d workers", currentTarget, newTargetCount)
			err := pool.stopWorkers(int(currentTarget - newTargetCount))
			if err != nil {
				log.Warn().Err(err).Msg("Failed to stop workers when scaling down")
			}
		} else {
			log.Info().Msgf("Utilization %.2f is above threshold %.2f, not scaling down", pool.utilization(), utilizationThreshold)
			return // Return early to avoid setting the target worker count
		}
	} else {
		log.Debug().Msgf("Worker pool already at target worker count %d", newTargetCount)
	}

	// Update the target worker count
	pool.workerCountTarget.Store(newTargetCount)
}

// busyStateWorkers returns two slices of worker IDs:
// 1. Busy workers
// 2. Idle workers
func (wp *workerPool) busyAndIdleWorkers() ([]xid.ID, []xid.ID) {
	var busyWorkers []xid.ID
	var idleWorkers []xid.ID
	wp.workers.Range(func(key, value any) bool {
		workerID := key.(xid.ID)
		workerInfo := value.(*workerInfo)
		if workerInfo.busy.Load() {
			busyWorkers = append(busyWorkers, workerID)
		} else {
			idleWorkers = append(idleWorkers, workerID)
		}
		return true
	})
	return busyWorkers, idleWorkers
}

// busyWorkers returns a slice of currently busy workers.
func (wp *workerPool) busyWorkers() []xid.ID {
	busyWorkers, _ := wp.busyAndIdleWorkers()
	return busyWorkers
}

// idleWorkers returns a slice of currently idle workers.
func (wp *workerPool) idleWorkers() []xid.ID {
	_, idleWorkers := wp.busyAndIdleWorkers()
	return idleWorkers
}

// processWorkerCountScaling listens for worker count requests and adjusts the worker count accordingly.
func (wp *workerPool) processWorkerCountScaling() {
	for {
		select {
		case <-wp.stopPoolChan:
			log.Debug().Msg("Worker count scaling received stop signal, exiting")
			return
		case requestWorkerCount := <-wp.workerCountChan:
			wp.adjustWorkerCount(requestWorkerCount)
		}
	}
}

// startWorker executes tasks from the task channel.
func (wp *workerPool) startWorker(id xid.ID) {
	log.Debug().Msgf("Starting worker %s", id)

	wp.workersRunning.Add(1)
	worker := &workerInfo{
		id:       id,
		busy:     atomic.Bool{},
		stopChan: make(chan struct{}),
	}
	wp.workers.Store(id, worker)

	defer func() {
		wp.workersRunning.Add(-1)
		wp.workers.Delete(id)
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

			// Update worker state
			worker.busy.Store(true)
			wp.workersActive.Add(1)

			// Execute the task
			start := time.Now()
			err := task.Execute()
			if err != nil {
				// No retry policy is implemented, we just log and send the error for now
				log.Debug().Err(err).Msgf("Worker %s: task execution failed", id)
				select {
				case wp.errorChan <- err:
					// Error sent successfully
				default:
					// Error channel not ready to receive, do nothing
				}
			}
			execTime := time.Since(start)
			select {
			case wp.execTimeChan <- execTime:
				// Execution time sent successfully
			default:
				// Execution time channel not ready to receive, log the exec time
				log.Debug().Msgf("Worker %s: exec time channel not ready to receive", id)
			}

			// Update worker state
			worker.busy.Store(false)
			wp.workersActive.Add(-1)
			log.Debug().Msgf("Worker %s: finished task", id)

		case <-worker.stopChan:
			log.Debug().Msgf("Worker %s: received targeted stop signal, exiting", id)
			return

		case <-wp.stopPoolChan:
			log.Debug().Msgf("Worker %s: received global stop signal, exiting", id)
			return
		}
	}
}

// stop signals the worker pool to stop processing tasks and exit.
func (wp *workerPool) stop() {
	log.Debug().Msg("Attempting worker pool stop")
	close(wp.stopPoolChan) // Signal workers to stop
	log.Debug().Msg("Waiting for workers to finish")
	wp.wg.Wait() // Wait for all workers to finish
	log.Debug().Msg("Worker pool stopped")
	close(wp.workerPoolDone) // Signal worker pool is done
}

// stopWorker signals a specific worker to stop processing tasks and exit. This will also remove
// the worker from the worker pool.
func (wp *workerPool) stopWorker(id xid.ID) error {
	log.Debug().Msgf("Stopping worker %s", id)
	value, ok := wp.workers.Load(id)
	if !ok {
		log.Debug().Msgf("Worker %s not found", id)
		return errors.New("worker not found")
	}
	workerInfo, ok := value.(*workerInfo)
	if !ok {
		log.Debug().Msgf("Worker %s has invalid type", id)
		return errors.New("worker has invalid type")
	}
	close(workerInfo.stopChan)
	log.Debug().Msgf("Stop signal sent for worker %s", id)
	return nil
}

// stopWorkers stops workers, which removes them from the pool.
// Note 1: if the number of workers to stop exceeds the number of idle workers the function will
// send stop signals to busy workers, which will stop after they finish their current task.
// Note 2: due to the timing of the stop signal, there is a chance that a worker marked as idle
// will pick up a task before the stop signal is received, in which case the worker will not stop
// until it finishes the task. This will not block this function.
func (wp *workerPool) stopWorkers(nWorkers int) error {
	wp.mu.Lock() // Lock to prevent race conditions while modifying worker state
	defer wp.mu.Unlock()

	// Validate number of workers to remove
	if nWorkers <= 0 {
		log.Debug().Msg("Cannot remove zero or negative workers")
		return errors.New("cannot remove zero or negative workers")
	}
	if nWorkers > int(wp.runningWorkers()) {
		log.Debug().Msg("Cannot remove more workers than are running")
		return errors.New("cannot remove more workers than are running")
	}
	log.Debug().Msgf("Removing %d workers from the pool", nWorkers)

	busyWorkers, idleWorkers := wp.busyAndIdleWorkers()

	// Stop a subset of the idle workers if there is an abundance
	if len(idleWorkers) >= nWorkers {
		workersToRemove := idleWorkers[:nWorkers]
		for _, workerID := range workersToRemove {
			err := wp.stopWorker(workerID)
			if err != nil {
				log.Debug().Err(err).Msgf("Failed to stop worker %s", workerID)
			}
		}
		return nil
	}

	// If there aren't enough idle workers, first stop all idle workers
	for _, workerID := range idleWorkers {
		err := wp.stopWorker(workerID)
		if err != nil {
			log.Debug().Err(err).Msgf("Failed to stop worker %s", workerID)
		}
	}

	// Then stop busy workers as well, up to the number of workers to remove
	nWorkers -= len(idleWorkers)
	busyWorkers = busyWorkers[:nWorkers]
	for _, workerID := range busyWorkers {
		err := wp.stopWorker(workerID)
		if err != nil {
			log.Debug().Err(err).Msgf("Failed to stop worker %s", workerID)
		}
	}

	return nil
}

// utilization returns the utilization of the worker pool as a float between 0.0 and 1.0.
func (wp *workerPool) utilization() float64 {
	if wp.runningWorkers() == 0 {
		return 0.0
	}
	return float64(wp.activeWorkers()) / float64(wp.runningWorkers())
}

// workerCountScalingChannel returns a write-only channel for scaling the worker count.
func (wp *workerPool) workerCountScalingChannel() chan<- int32 {
	return wp.workerCountChan
}

// newWorkerPool creates and returns a new worker pool.
func newWorkerPool(
	initialWorkers int,
	errorChan chan error,
	execTimeChan chan time.Duration,
	taskChan chan Task,
	workerPoolDone chan struct{},
) *workerPool {
	pool := &workerPool{
		errorChan:       errorChan,
		execTimeChan:    execTimeChan,
		stopPoolChan:    make(chan struct{}),
		taskChan:        taskChan,
		workerCountChan: make(chan int32),
		workerPoolDone:  workerPoolDone,
	}
	pool.addWorkers(initialWorkers)
	pool.workerCountTarget.Store(int32(initialWorkers))

	go pool.processWorkerCountScaling()

	return pool
}
