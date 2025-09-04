package taskman

import (
	"context"
	"sync"
	"time"
)

// PoolMetrics holds worker pool metrics.
type PoolMetrics struct {
	WidestJobWidth      int     // Widest job in the queue in terms of number of tasks
	WorkerCountTarget   int     // Target number of workers
	WorkerScalingEvents int     // Number of worker scaling events since start
	WorkerUtilization   float32 // Utilization of workers
	WorkersActive       int     // Number of active workers
	WorkersRunning      int     // Number of running workers
}

// metricsState stores the canonical metrics under lock.
type metricsState struct {
	JobsManaged   int64
	JobsPerSecond float32

	TasksManaged         int64
	TasksPerSecond       float32
	TasksAverageExecTime time.Duration
	TasksTotalExecutions int64
}

// executorMetrics stores metrics about task execution.
type executorMetrics struct {
	ctx    context.Context
	cancel context.CancelFunc

	mu sync.RWMutex
	s  metricsState
}

// consumeExecChan consumes execution times and calculates the average execution time of tasks.
func (m *executorMetrics) consumeExecChan(execChan <-chan time.Duration) {
	defer m.cancel()

	for {
		select {
		case execTime := <-execChan:
			m.consumeOneTaskExecution(execTime)
		case <-m.ctx.Done():
			// Only stop consuming once done is received
			return
		}
	}
}

// consumeOneTaskExecution updates the average execution time for a single observed task execution.
func (m *executorMetrics) consumeOneTaskExecution(execTime time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()

	total := m.s.TasksTotalExecutions
	avg := m.s.TasksAverageExecTime
	newAvg := (avg*time.Duration(total) + execTime) / time.Duration(total+1)
	m.s.TasksAverageExecTime = newAvg
	m.s.TasksTotalExecutions = total + 1
}

// snapshot returns a copy of the current metrics state under read lock.
func (m *executorMetrics) snapshot() metricsState {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.s
}

// taskSnapshot returns instantaneous signals for scaler reads.
// - tasksPerSec: current tasks/sec (float64)
// - taskAvgExecSec: average task execution time in seconds (float64)
// - tasks: number of managed tasks (int64)
func (m *executorMetrics) taskSnapshot() (tasksPerSec float64, taskAvgExecSec float64, tasks int64) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return float64(m.s.TasksPerSecond), m.s.TasksAverageExecTime.Seconds(), m.s.TasksManaged
}

// updateCadence updates the metrics for a change in cadence only. If called with
// newTaskCount != 0, the metrics will not be updated correctly.
func (m *executorMetrics) updateCadence(newTaskCount int, old, new time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()

	delta := (1/float32(new.Seconds()) - 1/float32(old.Seconds()))
	m.s.TasksPerSecond += float32(newTaskCount) * delta
	m.s.JobsPerSecond += delta
}

// updateMetrics updates the metrics. The input deltas correspond to number of tasks or jobs added
// or removed, and cadence is the cadence of the jobs and tasks affected by the change.
func (m *executorMetrics) updateMetrics(jobDelta, taskDelta int, cadence time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()

	newTaskCount := m.s.TasksManaged + int64(taskDelta)
	newJobCount := m.s.JobsManaged + int64(jobDelta)

	// Avoid division by zero and clamp counts to non-negative
	if newTaskCount <= 0 || newJobCount <= 0 {
		m.s.TasksPerSecond = 0
		m.s.JobsPerSecond = 0
		if newTaskCount < 0 {
			newTaskCount = 0
		}
		if newJobCount < 0 {
			newJobCount = 0
		}
		m.s.TasksManaged = newTaskCount
		m.s.JobsManaged = newJobCount
		return
	}

	tasksPerSecond := float32(taskDelta) / float32(cadence.Seconds())
	jobsPerSecond := float32(jobDelta) / float32(cadence.Seconds())

	m.s.TasksPerSecond += tasksPerSecond
	m.s.TasksManaged = newTaskCount
	m.s.JobsPerSecond += jobsPerSecond
	m.s.JobsManaged = newJobCount
}

// newExecutorMetrics creates a new executor metrics instance.
func newExecutorMetrics() *executorMetrics {
	ctx, cancel := context.WithCancel(context.Background())

	return &executorMetrics{
		ctx:    ctx,
		cancel: cancel,
	}
}
