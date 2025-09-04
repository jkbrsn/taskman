package taskman

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	uatomic "go.uber.org/atomic"
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

// executorMetrics stores metrics about task execution.
type executorMetrics struct {
	ctx    context.Context
	cancel context.CancelFunc

	mu sync.RWMutex

	jobsManaged   atomic.Int64    // Total number of jobs managed
	jobsPerSecond uatomic.Float32 // Number of jobs executed per second

	tasksManaged        atomic.Int64     // Total number of tasks managed
	tasksPerSecond      uatomic.Float32  // Number of tasks executed per second
	averageExecTime     uatomic.Duration // Average execution time of tasks
	totalTaskExecutions atomic.Int64     // Total number of tasks executed
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

	avgExecTime := m.averageExecTime.Load()
	taskExecutions := m.totalTaskExecutions.Load()
	newAvgExecTime := (avgExecTime*time.Duration(taskExecutions) + execTime) /
		time.Duration(taskExecutions+1)
	m.averageExecTime.Store(newAvgExecTime)
	m.totalTaskExecutions.Add(1)
}

// updateCadence updates the metrics for a change in cadence only. If called with
// newTaskCount != 0, the metrics will not be updated correctly.
func (m *executorMetrics) updateCadence(newTaskCount int, old, new time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()

	tasksPerSecondDelta := float32(newTaskCount) * (1/float32(new.Seconds()) - 1/float32(old.Seconds()))
	jobsPerSecondDelta := (1/float32(new.Seconds()) - 1/float32(old.Seconds()))

	// Store updated values
	m.tasksPerSecond.Add(tasksPerSecondDelta)
	m.jobsPerSecond.Add(jobsPerSecondDelta)
}

// updateMetrics updates the metrics. The input deltas correspond to number of tasks or jobs added
// or removed, and cadence is the cadence of the jobs and tasks affected by the change.
func (m *executorMetrics) updateMetrics(jobDelta, taskDelta int, cadence time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Calculate the new number of tasks in the queue
	currentTaskCount := m.tasksManaged.Load()
	newTaskCount := currentTaskCount + int64(taskDelta)
	currentJobCount := m.jobsManaged.Load()
	newJobCount := currentJobCount + int64(jobDelta)

	// Avoid division by zero
	if newTaskCount <= 0 || newJobCount <= 0 {
		m.tasksPerSecond.Store(0)
		m.jobsPerSecond.Store(0)
		m.tasksManaged.Store(max(newTaskCount, 0))
		m.jobsManaged.Store(max(newJobCount, 0))
		return
	}

	// Calculate execution per second contributions
	tasksPerSecond := float32(taskDelta) / float32(cadence.Seconds())
	jobsPerSecond := float32(jobDelta) / float32(cadence.Seconds())

	// Store updated values
	m.tasksPerSecond.Add(tasksPerSecond)
	m.tasksManaged.Add(int64(taskDelta))
	m.jobsPerSecond.Add(jobsPerSecond)
	m.jobsManaged.Add(int64(jobDelta))
}

// newExecutorMetrics creates a new executor metrics instance.
func newExecutorMetrics() *executorMetrics {
	ctx, cancel := context.WithCancel(context.Background())

	return &executorMetrics{
		ctx:    ctx,
		cancel: cancel,
	}
}
