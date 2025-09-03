package taskman

import (
	"context"
	"math"
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

// consumeExecTime consumes execution times and calculates the average execution time of tasks.
func (m *executorMetrics) consumeExecTime(execTimeChan <-chan time.Duration) {
	defer m.cancel()

	for {
		select {
		case execTime := <-execTimeChan:
			m.consumeOneExecTime(execTime)
		case <-m.ctx.Done():
			// Only stop consuming once done is received
			return
		}
	}
}

// consumeOneExecTime updates the average execution time for a single observed execution.
func (m *executorMetrics) consumeOneExecTime(execTime time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()

	avgExecTime := m.averageExecTime.Load()
	taskExecutions := m.totalTaskExecutions.Load()
	newAvgExecTime := (avgExecTime*time.Duration(taskExecutions) + execTime) /
		time.Duration(taskExecutions+1)
	m.averageExecTime.Store(newAvgExecTime)
	m.totalTaskExecutions.Add(1)
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

	// Calculate the new tasks per second
	tasksPerSecond := eventsPerSecond(taskDelta, cadence)
	jobsPerSecond := eventsPerSecond(jobDelta, cadence)

	// Update the tasks per second metric base on a weighted average
	newTasksPerSecond := (tasksPerSecond*float32(taskDelta) +
		m.tasksPerSecond.Load()*float32(currentTaskCount)) /
		float32(newTaskCount)

	// Update the jobs per second metric base on a weighted average
	newJobsPerSecond := (jobsPerSecond*float32(jobDelta) +
		m.jobsPerSecond.Load()*float32(currentJobCount)) /
		float32(newJobCount)

	// Store updated values
	m.tasksPerSecond.Store(newTasksPerSecond)
	m.tasksManaged.Add(int64(taskDelta))
	m.jobsPerSecond.Store(newJobsPerSecond)
	m.jobsManaged.Add(int64(jobDelta))
}

// eventsPerSecond calculates the number of events executed per second.
func eventsPerSecond(n int, cadence time.Duration) float32 {
	if cadence == 0 {
		return 0
	}
	events := math.Abs(float64(n))
	return float32(events) / float32(cadence.Seconds())
}

// newExecutorMetrics creates a new executor metrics instance.
func newExecutorMetrics() *executorMetrics {
	ctx, cancel := context.WithCancel(context.Background())

	return &executorMetrics{
		ctx:    ctx,
		cancel: cancel,
	}
}
