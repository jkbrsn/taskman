// Package taskman provides a simple task scheduler with per-job runners.
package taskman

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jkbrsn/threadsafe"
	"github.com/rs/zerolog"
)

// distributedExecutor is an implementation of executor that runs tasks in separate goroutines.
type distributedExecutor struct {
	log zerolog.Logger

	ctx    context.Context
	cancel context.CancelFunc

	runners threadsafe.Map[string, *jobRunner] // jobID -> runner

	errCh   chan error
	metrics *managerMetrics

	// Configurable options
	catchUpMax int  // max immediate catch-ups per tick when behind schedule
	parallel   bool // run tasks in parallel within each job
	maxPar     int  // parallelism limit per job (0 = unlimited)
}

// Job returns the job with the given ID.
func (e *distributedExecutor) Job(jobID string) (Job, error) {
	runner, ok := e.runners.Get(jobID)
	if !ok {
		return Job{}, fmt.Errorf("job with ID %s not found", jobID)
	}
	runner.mu.RLock()
	defer runner.mu.RUnlock()

	return runner.job, nil
}

// Metrics returns the metrics for the distributed executor.
func (e *distributedExecutor) Metrics() TaskManagerMetrics {
	return TaskManagerMetrics{
		ManagedJobs:          int(e.metrics.jobsManaged.Load()),
		JobsPerSecond:        e.metrics.jobsPerSecond.Load(),
		ManagedTasks:         int(e.metrics.tasksManaged.Load()),
		TasksPerSecond:       e.metrics.tasksPerSecond.Load(),
		TaskAverageExecTime:  time.Duration(e.metrics.averageExecTime.Load()),
		TasksTotalExecutions: int(e.metrics.totalTaskExecutions.Load()),
		PoolMetrics:          nil,
	}
}

// Remove removes a job.
func (e *distributedExecutor) Remove(jobID string) error {
	runner, ok := e.runners.Get(jobID)
	if !ok {
		return fmt.Errorf("job with ID %s not found", jobID)
	}
	e.runners.Delete(jobID)

	runner.cancel()
	runner.wg.Wait()

	e.metrics.updateTaskMetrics(-1, -len(runner.job.Tasks), runner.job.Cadence)
	return nil
}

// Replace replaces a job.
func (e *distributedExecutor) Replace(job Job) error {
	runner, ok := e.runners.Get(job.ID)
	if !ok {
		return errors.New("job not found")
	}

	// Preserve schedule position and update metrics
	runner.mu.Lock()
	prev := runner.job
	job.NextExec = prev.NextExec
	runner.job = job
	runner.mu.Unlock()

	// Update metrics: jobs count unchanged, adjust tasks managed by delta
	delta := len(job.Tasks) - len(prev.Tasks)
	if delta != 0 {
		e.metrics.updateTaskMetrics(0, delta, job.Cadence)
	}

	return nil
}

// Schedule schedules a new job.
func (e *distributedExecutor) Schedule(job Job) error {
	if err := job.Validate(); err != nil {
		return fmt.Errorf("invalid job: %w", err)
	}

	if _, exists := e.runners.Get(job.ID); exists {
		return errors.New("duplicate job ID")
	}

	rCtx, rCancel := context.WithCancel(e.ctx)
	runner := &jobRunner{
		job:        job,
		ctx:        rCtx,
		cancel:     rCancel,
		parallel:   e.parallel,
		maxPar:     e.maxPar,
		catchUpMax: e.catchUpMax,
	}
	runner.wg.Add(1)
	go runner.loop(e.errCh, e.metrics)

	e.runners.Set(job.ID, runner)
	e.metrics.updateTaskMetrics(1, len(job.Tasks), job.Cadence)

	return nil
}

// Start is a no-op for the distributed executor, as each job is started by its runner when
// scheduled.
func (*distributedExecutor) Start() {}

// Stop stops the distributed executor by sending a cancel signal to all runners.
func (e *distributedExecutor) Stop() {
	e.cancel()

	// wait for all to finish
	e.runners.Range(func(key string, value *jobRunner) bool {
		value.wg.Wait()
		return true
	})
}

// jobRunner is an implementation of job runner that runs tasks in a separate goroutine.
type jobRunner struct {
	mu sync.RWMutex

	job    Job
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	parallel   bool // run tasks in parallel within a job
	maxPar     int  // 0 = unlimited
	catchUpMax int  // max immediate catch-ups per tick when behind
}

func (r *jobRunner) execute(errCh chan<- error, metrics *managerMetrics) {
	if r.parallel {
		r.runParallel(errCh, metrics)
		return
	}
	r.runSequential(errCh, metrics)
}

// loop runs the job runner.
//
// It uses a one-shot timer that always points at the job's next scheduled execution time
// (stored in the local "next" variable). After each run, "next" is advanced by the
// cadence. If the runner fell behind schedule (e.g., the previous run took longer than
// one cadence), we allow it to "catch up" by at most catchUpMax skipped periods to avoid
// a long backlog of immediate executions.
func (r *jobRunner) loop(errCh chan<- error, metrics *managerMetrics) {
	defer r.wg.Done()

	// Initialize the first fire time from the job's schedule.
	r.mu.RLock()
	next := r.job.NextExec
	r.mu.RUnlock()
	timer := time.NewTimer(time.Until(next))
	defer timer.Stop()

	// Limit how many missed cadences we execute back-to-back when behind schedule.
	catchUpMax := r.catchUpMax
	if catchUpMax <= 0 {
		catchUpMax = 1
	}

	for {
		select {
		case <-r.ctx.Done():
			return
		case <-timer.C:
			start := time.Now()

			// Execute tasks for this job tick.
			r.execute(errCh, metrics)

			metrics.consumeOneExecTime(time.Since(start))

			// Advance "next" forward by whole cadences until it lands in the future,
			// but cap the number of immediate catch-ups to catchUpMax.
			skips := 0
			r.mu.RLock()
			cadence := r.job.Cadence
			r.mu.RUnlock()
			for {
				next = next.Add(cadence)
				if skips >= catchUpMax || next.After(time.Now()) {
					break
				}
				skips++
			}

			// Reset the timer to fire at the upcoming "next" (never negative duration).
			duration := max(time.Until(next), 0)

			// Drain the timer channel if needed before resetting to avoid spurious wakeups.
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(duration)
		}
	}
}

// runSequential runs the job sequentially.
func (r *jobRunner) runSequential(errCh chan<- error, metrics *managerMetrics) {
	r.mu.RLock()
	tasks := r.job.Tasks
	r.mu.RUnlock()

	for _, t := range tasks {
		if r.ctx.Err() != nil {
			return
		}
		func() {
			defer func() {
				if rec := recover(); rec != nil {
					// Report panic as error but keep runner alive
					select {
					case errCh <- fmt.Errorf("runner %s: panic: %v", r.job.ID, rec):
					default:
					}
				}
			}()
			if err := t.Execute(); err != nil {
				select {
				case errCh <- err:
				default:
				}
			}
			metrics.totalTaskExecutions.Add(1)
		}()
	}
}

// runParallel runs the job in parallel.
func (r *jobRunner) runParallel(errCh chan<- error, metrics *managerMetrics) {
	var wg sync.WaitGroup
	var sem chan struct{}

	r.mu.RLock()
	maxPar := r.maxPar
	tasks := r.job.Tasks
	r.mu.RUnlock()

	if maxPar > 0 {
		sem = make(chan struct{}, maxPar)
	}

	for _, t := range tasks {
		if r.ctx.Err() != nil {
			break
		}
		if sem != nil {
			sem <- struct{}{}
		}
		wg.Add(1)
		go func(tt Task) {
			defer wg.Done()
			defer func() {
				if rec := recover(); rec != nil {
					select {
					case errCh <- fmt.Errorf("runner %s: panic: %v", r.job.ID, rec):
					default:
					}
				}
			}()
			if err := tt.Execute(); err != nil {
				select {
				case errCh <- err:
				default:
				}
			}
			metrics.totalTaskExecutions.Add(1)
			if sem != nil {
				<-sem
			}
		}(t)
	}

	wg.Wait()
}

// equalRunners returns true if the two runners have the same job ID.
func equalRunners(r1, r2 *jobRunner) bool {
	r1.mu.RLock()
	defer r1.mu.RUnlock()

	r2.mu.RLock()
	defer r2.mu.RUnlock()

	return r1.job.ID == r2.job.ID
}

// newDistributedExecutor creates a new distributed executor.
func newDistributedExecutor(
	parent context.Context,
	logger zerolog.Logger,
	errCh chan error,
	metrics *managerMetrics,
	catchUpMax int,
	parallel bool,
	maxPar int,
) *distributedExecutor {
	ctx, cancel := context.WithCancel(parent)
	log := logger.With().Str("component", "executor").Logger()

	return &distributedExecutor{
		log:        log,
		ctx:        ctx,
		cancel:     cancel,
		runners:    threadsafe.NewRWMutexMap[string](equalRunners),
		errCh:      errCh,
		metrics:    metrics,
		catchUpMax: catchUpMax,
		parallel:   parallel,
		maxPar:     maxPar,
	}
}
