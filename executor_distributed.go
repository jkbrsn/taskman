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

	pausedMu      sync.RWMutex
	pausedRunners map[string]pausedRunner

	errCh        chan error
	taskExecChan chan time.Duration
	jobExecChan  chan struct{}
	metrics      *executorMetrics

	// Configurable options
	catchUpMax int  // max immediate catch-ups per tick when behind schedule
	parallel   bool // run tasks in parallel within each job
	maxPar     int  // parallelism limit per job (0 = unlimited)
}

// pausedRunner represents a paused job runner.
type pausedRunner struct {
	job       Job
	remaining time.Duration
}

// Job returns the job with the given ID.
func (e *distributedExecutor) Job(jobID string) (Job, error) {
	runner, ok := e.runners.Get(jobID)
	if !ok {
		e.pausedMu.RLock()
		paused, pausedOK := e.pausedRunners[jobID]
		e.pausedMu.RUnlock()
		if pausedOK {
			job := paused.job
			job.NextExec = time.Now().Add(paused.remaining)
			return job, nil
		}
		return Job{}, fmt.Errorf("job with ID %s not found", jobID)
	}
	runner.mu.RLock()
	defer runner.mu.RUnlock()

	return runner.job, nil
}

// Metrics returns the metrics for the distributed executor.
func (e *distributedExecutor) Metrics() TaskManagerMetrics {
	snap := e.metrics.snapshot()
	return TaskManagerMetrics{
		ManagedJobs:          int(snap.JobsManaged),
		JobsPerSecond:        snap.JobsPerSecond,
		JobsTotalExecutions:  int(snap.JobsTotalExecutions),
		ManagedTasks:         int(snap.TasksManaged),
		TasksPerSecond:       snap.TasksPerSecond,
		TasksAverageExecTime: snap.TasksAverageExecTime,
		TasksTotalExecutions: int(snap.TasksTotalExecutions),
		PoolMetrics:          nil,
	}
}

// Remove removes a job.
func (e *distributedExecutor) Remove(jobID string) error {
	runner, ok := e.runners.Get(jobID)
	if !ok {
		e.pausedMu.Lock()
		if paused, ok := e.pausedRunners[jobID]; ok {
			delete(e.pausedRunners, jobID)
			e.pausedMu.Unlock()
			e.metrics.updateMetrics(-1, -len(paused.job.Tasks), paused.job.Cadence)
			return nil
		}
		e.pausedMu.Unlock()
		return fmt.Errorf("job with ID %s not found", jobID)
	}
	e.runners.Delete(jobID)

	runner.cancel()
	runner.wg.Wait()

	e.metrics.updateMetrics(-1, -len(runner.job.Tasks), runner.job.Cadence)
	return nil
}

// Pause pauses a job.
func (e *distributedExecutor) Pause(jobID string) error {
	select {
	case <-e.ctx.Done():
		return ErrExecutorContextDone
	default:
	}

	e.pausedMu.Lock()
	if _, exists := e.pausedRunners[jobID]; exists {
		e.pausedMu.Unlock()
		return fmt.Errorf("job %s already paused", jobID)
	}
	e.pausedMu.Unlock()

	runner, ok := e.runners.Get(jobID)
	if !ok {
		return fmt.Errorf("pause job %q: job not found", jobID)
	}
	e.runners.Delete(jobID)

	runner.mu.RLock()
	jobCopy := runner.job
	runner.mu.RUnlock()

	remaining := max(time.Until(jobCopy.NextExec), 0)

	runner.cancel()
	runner.wg.Wait()

	e.pausedMu.Lock()
	e.pausedRunners[jobID] = pausedRunner{
		job:       jobCopy,
		remaining: remaining,
	}
	e.pausedMu.Unlock()

	return nil
}

// Resume resumes a paused job.
func (e *distributedExecutor) Resume(jobID string) error {
	select {
	case <-e.ctx.Done():
		return ErrExecutorContextDone
	default:
	}

	e.pausedMu.Lock()
	paused, ok := e.pausedRunners[jobID]
	if !ok {
		e.pausedMu.Unlock()
		return fmt.Errorf("job %s is not paused", jobID)
	}
	delete(e.pausedRunners, jobID)
	e.pausedMu.Unlock()

	job := paused.job
	job.NextExec = time.Now().Add(paused.remaining)
	job.initializeExecLimit()

	rCtx, rCancel := context.WithCancel(e.ctx)
	runner := &jobRunner{
		job:        job,
		ctx:        rCtx,
		cancel:     rCancel,
		parallel:   e.parallel,
		maxPar:     e.maxPar,
		catchUpMax: e.catchUpMax,
		exec:       e,
	}

	// Register the runner before starting its goroutine to avoid a race where the
	// runner completes immediately and attempts to remove itself before it is
	// present in the runners map.
	e.runners.Set(job.ID, runner)
	runner.wg.Add(1)
	go runner.loop(e.errCh, e.taskExecChan, e.jobExecChan)

	return nil
}

// Replace replaces a job.
func (e *distributedExecutor) Replace(job Job) error {
	if err := job.Validate(); err != nil {
		return fmt.Errorf("invalid job: %w", err)
	}

	runner, ok := e.runners.Get(job.ID)
	if !ok {
		return errors.New("job not found")
	}

	// Preserve schedule position and update metrics
	runner.mu.Lock()
	prev := runner.job
	job.NextExec = prev.NextExec
	job.inheritExecLimit(&prev)
	runner.job = job
	runner.mu.Unlock()

	// Update metrics: jobs count unchanged, adjust tasks managed by delta
	delta := len(job.Tasks) - len(prev.Tasks)
	if delta != 0 {
		e.metrics.updateMetrics(0, delta, job.Cadence)
	} else if prev.Cadence != job.Cadence {
		e.metrics.updateCadence(len(job.Tasks), prev.Cadence, job.Cadence)
	}

	return nil
}

// Schedule schedules a new job.
func (e *distributedExecutor) Schedule(job Job) error {
	if err := job.Validate(); err != nil {
		return fmt.Errorf("invalid job: %w", err)
	}

	job.initializeExecLimit()

	// Check executor context state
	select {
	case <-e.ctx.Done():
		// If the executor is stopped, do not continue adding the job
		return ErrExecutorContextDone
	default:
		// Pass through if the executor is running
	}

	if _, exists := e.runners.Get(job.ID); exists {
		return errors.New("duplicate job ID")
	}
	e.pausedMu.RLock()
	if _, exists := e.pausedRunners[job.ID]; exists {
		e.pausedMu.RUnlock()
		return errors.New("duplicate job ID")
	}
	e.pausedMu.RUnlock()

	e.log.Debug().Msgf(
		"Scheduling job with %d tasks with ID '%s' and cadence %v",
		len(job.Tasks), job.ID, job.Cadence,
	)

	// Set NextExec to now if it is not set
	if job.NextExec.IsZero() {
		job.NextExec = time.Now().Add(job.Cadence)
	}

	rCtx, rCancel := context.WithCancel(e.ctx)
	runner := &jobRunner{
		job:        job,
		ctx:        rCtx,
		cancel:     rCancel,
		parallel:   e.parallel,
		maxPar:     e.maxPar,
		catchUpMax: e.catchUpMax,
		exec:       e,
	}

	// Register the runner before starting its goroutine to avoid a race where the
	// runner completes immediately and attempts to remove itself before it is
	// present in the runners map.
	e.runners.Set(job.ID, runner)
	runner.wg.Add(1)
	go runner.loop(e.errCh, e.taskExecChan, e.jobExecChan)

	e.metrics.updateMetrics(1, len(job.Tasks), job.Cadence)

	return nil
}

// Start is a no-op for the distributed executor, as each job is started by its runner when
// scheduled.
func (e *distributedExecutor) Start() {
	// Metrics: reuse provided metrics but validate context
	if e.metrics == nil {
		e.metrics = newExecutorMetrics()
	} else if e.metrics.ctx == nil {
		ctx, cancel := context.WithCancel(context.Background())
		e.metrics.ctx = ctx
		e.metrics.cancel = cancel
	}

	go e.metrics.consumeTaskExecChan(e.taskExecChan)
	go e.metrics.consumeJobExecChan(e.jobExecChan)
}

// Stop stops the distributed executor by sending a cancel signal to all runners.
func (e *distributedExecutor) Stop() {
	e.cancel()

	// wait for all to finish
	e.runners.Range(func(_ string, value *jobRunner) bool {
		value.wg.Wait()
		return true
	})

	e.pausedMu.Lock()
	e.pausedRunners = make(map[string]pausedRunner)
	e.pausedMu.Unlock()

	// Close execution channel
	close(e.taskExecChan)
	close(e.jobExecChan)

	// Stop metrics
	e.metrics.cancel()
}

// completeRunner removes a job runner from the executor and updates metrics.
func (e *distributedExecutor) completeRunner(r *jobRunner) {
	if r == nil {
		return
	}
	job := r.snapshotJob()
	r.cancel()
	if _, ok := e.runners.LoadAndDelete(job.ID); ok {
		e.metrics.updateMetrics(-1, -len(job.Tasks), job.Cadence)
	}
}

// jobRunner is an implementation of job runner that runs tasks in a separate goroutine.
type jobRunner struct {
	mu sync.RWMutex

	job    Job
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	exec   *distributedExecutor

	parallel   bool // run tasks in parallel within a job
	maxPar     int  // 0 = unlimited
	catchUpMax int  // max immediate catch-ups per tick when behind
}

// execute runs the job runner.
func (r *jobRunner) execute(errCh chan<- error, taskExecChan chan<- time.Duration) {
	if r.parallel {
		r.runParallel(errCh, taskExecChan)
		return
	}
	r.runSequential(errCh, taskExecChan)
}

// loop runs the job runner.
//
// It uses a one-shot timer that always points at the job's next scheduled execution time
// (stored in the local "next" variable). After each run, "next" is advanced by the
// cadence. If the runner fell behind schedule (e.g., the previous run took longer than
// one cadence), we allow it to "catch up" by at most catchUpMax skipped periods to avoid
// a long backlog of immediate executions.
// loop runs the job runner.
//
// It uses a one-shot timer that always points at the job's next scheduled execution time
// (stored in the local "next" variable). After each run, "next" is advanced by the
// cadence. If the runner fell behind schedule (e.g., the previous run took longer than
// one cadence), we allow it to "catch up" by at most catchUpMax skipped periods to avoid
// a long backlog of immediate executions.
func (r *jobRunner) loop(
	errCh chan<- error,
	taskExecChan chan<- time.Duration,
	jobExecChan chan<- struct{},
) {
	defer r.wg.Done()

	// Initialize the first fire time from the job's schedule.
	r.mu.RLock()
	next := r.job.NextExec
	r.mu.RUnlock()

	r.mu.Lock()
	r.job.NextExec = next
	r.mu.Unlock()
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
			// Handle the tick in a helper to keep cognitive complexity low.
			if r.handleTimerTick(errCh, taskExecChan, jobExecChan, &next, catchUpMax) {
				return
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

// handleTimerTick performs the work that used to be in the timer.C case of loop.
// It returns true when the runner should stop (job exhausted or context canceled).
func (r *jobRunner) handleTimerTick(
	errCh chan<- error,
	taskExecChan chan<- time.Duration,
	jobExecChan chan<- struct{},
	next *time.Time,
	catchUpMax int,
) bool {
	// Execute tasks for this job tick.
	r.execute(errCh, taskExecChan)

	// Meter one job execution
	jobExecChan <- struct{}{}

	// Consume one run and check if the job is done.
	r.mu.Lock()
	cadence := r.job.Cadence
	jobDone := r.job.consumeRun()
	r.mu.Unlock()
	if jobDone {
		if r.exec != nil {
			r.exec.completeRunner(r)
		}
		return true
	}

	// Advance "next" forward by whole cadences until it lands in the future,
	// but cap the number of immediate catch-ups to catchUpMax.
	skips := 0
	now := time.Now()
	for {
		*next = (*next).Add(cadence)
		if skips >= catchUpMax || (*next).After(now) {
			break
		}
		skips++
	}

	// Persist next to the job under lock.
	r.mu.Lock()
	r.job.NextExec = *next
	r.mu.Unlock()

	return false
}

// runSequential runs the job sequentially.
func (r *jobRunner) runSequential(errCh chan<- error, taskExecChan chan<- time.Duration) {
	r.mu.RLock()
	tasks := r.job.Tasks
	jobID := r.job.ID
	r.mu.RUnlock()

	for _, t := range tasks {
		safeExecuteTask(r.ctx, jobID, t, errCh, taskExecChan)
	}
}

// runParallel runs the job in parallel.
func (r *jobRunner) runParallel(errCh chan<- error, taskExecChan chan<- time.Duration) {
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
		go func(tt Task, jobID string) {
			defer wg.Done()
			safeExecuteTask(r.ctx, jobID, tt, errCh, taskExecChan)
			if sem != nil {
				<-sem
			}
		}(t, r.job.ID)
	}

	wg.Wait()
}

func (r *jobRunner) snapshotJob() Job {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.job
}

// equalRunners returns true if the two runners have the same job ID.
func equalRunners(r1, r2 *jobRunner) bool {
	r1.mu.RLock()
	defer r1.mu.RUnlock()

	r2.mu.RLock()
	defer r2.mu.RUnlock()

	return r1.job.ID == r2.job.ID
}

// safeExecuteTask executes one task with panic recovery and metrics update.
func safeExecuteTask(
	ctx context.Context,
	jobID string,
	t Task,
	errCh chan<- error,
	taskExecChan chan<- time.Duration,
) {
	if ctx.Err() != nil {
		return
	}
	defer func() {
		if rec := recover(); rec != nil {
			select {
			case errCh <- fmt.Errorf("runner %s: panic: %v", jobID, rec):
			default:
			}
		}
	}()

	start := time.Now()
	if err := t.Execute(); err != nil {
		select {
		case errCh <- err:
		default:
		}
	}
	taskExecChan <- time.Since(start)
}

// newDistributedExecutor creates a new distributed executor.
func newDistributedExecutor(
	parent context.Context,
	logger zerolog.Logger,
	errCh chan error,
	metrics *executorMetrics,
	channelBufferSize int,
	catchUpMax int,
	parallel bool,
	maxPar int,
) *distributedExecutor {
	ctx, cancel := context.WithCancel(parent)
	log := logger.With().Str("component", "executor").Logger()

	return &distributedExecutor{
		log:           log,
		ctx:           ctx,
		cancel:        cancel,
		runners:       threadsafe.NewRWMutexMap[string](equalRunners),
		pausedRunners: make(map[string]pausedRunner),
		errCh:         errCh,
		taskExecChan:  make(chan time.Duration, channelBufferSize),
		jobExecChan:   make(chan struct{}, channelBufferSize),
		metrics:       metrics,
		catchUpMax:    catchUpMax,
		parallel:      parallel,
		maxPar:        maxPar,
	}
}
