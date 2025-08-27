// Package taskman provides a simple task scheduler with per-job runners.
package taskman

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/rs/zerolog"
)

// distributedExecutor is an implementation of executor that runs tasks in separate goroutines.
type distributedExecutor struct {
	log zerolog.Logger

	ctx    context.Context
	cancel context.CancelFunc

	mu      sync.RWMutex
	runners map[string]*jobRunner // jobID -> runner

	errCh chan error
	met   *managerMetrics

	// Configurable options
	catchUpMax int  // max immediate catch-ups per tick when behind schedule
	parallel   bool // run tasks in parallel within each job
	maxPar     int  // parallelism limit per job (0 = unlimited)
}

// Metrics returns the metrics for the distributed executor.
func (e *distributedExecutor) Metrics() TaskManagerMetrics {
	return TaskManagerMetrics{
		ManagedJobs:          int(e.met.jobsManaged.Load()),
		ManagedTasks:         int(e.met.tasksManaged.Load()),
		TaskAverageExecTime:  time.Duration(e.met.averageExecTime.Load()),
		TasksTotalExecutions: int(e.met.totalTaskExecutions.Load()),
		TasksPerSecond:       e.met.tasksPerSecond.Load(),
		PoolMetrics:          nil,
	}
}

// Remove removes a job.
func (e *distributedExecutor) Remove(jobID string) error {
	e.mu.Lock()
	r, ok := e.runners[jobID]
	if !ok {
		e.mu.Unlock()
		return fmt.Errorf("job with ID %s not found", jobID)
	}
	delete(e.runners, jobID)
	e.mu.Unlock()

	r.cancel()
	r.wg.Wait()

	// adjust metrics (negative counts)
	e.met.updateTaskMetrics(-1, -len(r.job.Tasks), r.job.Cadence)
	return nil
}

// Replace replaces a job.
func (e *distributedExecutor) Replace(j Job) error {
	e.mu.Lock()
	r, ok := e.runners[j.ID]
	if !ok {
		e.mu.Unlock()
		return errors.New("job not found")
	}
	// preserve schedule position
	j.NextExec = r.job.NextExec
	r.job = j
	e.mu.Unlock()
	// metrics: if task width changed, update maxJobWidth & tasksInQueue deltas
	return nil
}

// Schedule schedules a new job.
func (e *distributedExecutor) Schedule(j Job) error {
	if err := j.Validate(); err != nil {
		return fmt.Errorf("invalid job: %w", err)
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	if _, exists := e.runners[j.ID]; exists {
		return errors.New("duplicate job ID")
	}

	rCtx, rCancel := context.WithCancel(e.ctx)
	r := &jobRunner{
		job:        j,
		ctx:        rCtx,
		cancel:     rCancel,
		parallel:   e.parallel,
		maxPar:     e.maxPar,
		catchUpMax: e.catchUpMax,
	}
	r.wg.Add(1)
	go r.loop(e.errCh, e.met)
	e.runners[j.ID] = r

	// metrics
	e.met.updateTaskMetrics(1, len(j.Tasks), j.Cadence)
	return nil
}

// Start is a no-op for the distributed executor, as each job is started by its runner when
// scheduled.
func (*distributedExecutor) Start() {}

// Stop stops the distributed executor by fanning out to all runners.
func (e *distributedExecutor) Stop() {
	e.mu.RLock()
	for _, r := range e.runners {
		r.cancel()
	}
	e.mu.RUnlock()
	e.cancel()

	// wait for all to finish
	e.mu.RLock()
	for _, r := range e.runners {
		r.wg.Wait()
	}
	e.mu.RUnlock()
}

// jobRunner is an implementation of job runner that runs tasks in a separate goroutine.
type jobRunner struct {
	job    Job
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	parallel   bool // run tasks in parallel within a job
	maxPar     int  // 0 = unlimited
	catchUpMax int  // max immediate catch-ups per tick when behind
}

func (r *jobRunner) execute(errCh chan<- error, met *managerMetrics) {
	if r.parallel {
		r.runParallel(errCh, met)
		return
	}
	r.runSequential(errCh, met)
}

// loop runs the job runner.
//
// It uses a one-shot timer that always points at the job's next scheduled execution time
// (stored in the local "next" variable). After each run, "next" is advanced by the
// cadence. If the runner fell behind schedule (e.g., the previous run took longer than
// one cadence), we allow it to "catch up" by at most catchUpMax skipped periods to avoid
// a long backlog of immediate executions.
func (r *jobRunner) loop(errCh chan<- error, met *managerMetrics) {
	defer r.wg.Done()

	// Initialize the first fire time from the job's schedule.
	next := r.job.NextExec
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
			r.execute(errCh, met)

			met.consumeOneExecTime(time.Since(start))

			// Advance "next" forward by whole cadences until it lands in the future,
			// but cap the number of immediate catch-ups to catchUpMax.
			skips := 0
			for {
				next = next.Add(r.job.Cadence)
				if skips >= catchUpMax || next.After(time.Now()) {
					break
				}
				skips++
			}

			// Reset the timer to fire at the upcoming "next" (never negative duration).
			d := max(time.Until(next), 0)

			// Drain the timer channel if needed before resetting to avoid spurious wakeups.
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(d)
		}
	}
}

// runSequential runs the job sequentially.
func (r *jobRunner) runSequential(errCh chan<- error, met *managerMetrics) {
	for _, t := range r.job.Tasks {
		if r.ctx.Err() != nil {
			return
		}
		if err := t.Execute(); err != nil {
			select {
			case errCh <- err:
			default:
			}
		}
		met.totalTaskExecutions.Add(1)
	}
}

// runParallel runs the job in parallel.
func (r *jobRunner) runParallel(errCh chan<- error, met *managerMetrics) {
	var wg sync.WaitGroup
	var sem chan struct{}
	if r.maxPar > 0 {
		sem = make(chan struct{}, r.maxPar)
	}
	for _, t := range r.job.Tasks {
		if r.ctx.Err() != nil {
			break
		}
		if sem != nil {
			sem <- struct{}{}
		}
		wg.Add(1)
		go func(tt Task) {
			defer wg.Done()
			if err := tt.Execute(); err != nil {
				select {
				case errCh <- err:
				default:
				}
			}
			met.totalTaskExecutions.Add(1)
			if sem != nil {
				<-sem
			}
		}(t)
	}
	wg.Wait()
}

func newDistributedExecutor(
	parent context.Context,
	logger zerolog.Logger,
	errCh chan error,
	met *managerMetrics,
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
		runners:    make(map[string]*jobRunner),
		errCh:      errCh,
		met:        met,
		catchUpMax: catchUpMax,
		parallel:   parallel,
		maxPar:     maxPar,
	}
}
