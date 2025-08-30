package taskman

import (
	"math"
	"sync/atomic"
	"time"

	uatomic "go.uber.org/atomic"
)

// TaskManagerMetrics holds metrics about various aspects of the task manager.
type TaskManagerMetrics struct {
	// Jobs
	ManagedJobs   int     // Total number of jobs in the queue
	JobsPerSecond float32 // Number of jobs executed per second

	// Tasks
	ManagedTasks         int           // Total number of tasks in the queue
	TasksPerSecond       float32       // Number of tasks executed per second
	TaskAverageExecTime  time.Duration // Average execution time of tasks
	TasksTotalExecutions int           // Total number of tasks executed

	// Worker pool
	PoolMetrics *PoolMetrics

	// TODO: consider adding:
	// JobsPerSecond float32
	// JobSuccessRate
	// JobLatency
	// JobBacklog
	// TaskSuccessRate
	// TaskLatency
	// TaskQueueWait
	// TaskBacklogLength
	// WorkerAverageLifetime
}

// PoolMetrics holds worker pool metrics.
type PoolMetrics struct {
	WidestJobWidth      int     // Widest job in the queue in terms of number of tasks
	WorkerCountTarget   int     // Target number of workers
	WorkerScalingEvents int     // Number of worker scaling events since start
	WorkerUtilization   float32 // Utilization of workers
	WorkersActive       int     // Number of active workers
	WorkersRunning      int     // Number of running workers
}

// managerMetrics stores internal metrics about the task manager.
type managerMetrics struct {
	jobsManaged   atomic.Int64    // Total number of jobs managed
	jobsPerSecond uatomic.Float32 // Number of jobs executed per second

	tasksManaged        atomic.Int64     // Total number of tasks managed
	tasksPerSecond      uatomic.Float32  // Number of tasks executed per second
	averageExecTime     uatomic.Duration // Average execution time of tasks
	totalTaskExecutions atomic.Int64     // Total number of tasks executed

	done <-chan struct{}
}

// consumeExecTime consumes execution times and calculates the average execution time of tasks.
func (mm *managerMetrics) consumeExecTime(execTimeChan <-chan time.Duration) {
	for {
		select {
		case execTime := <-execTimeChan:
			mm.consumeOneExecTime(execTime)
		case <-mm.done:
			// Only stop consuming once done is received
			return
		}
	}
}

// consumeOneExecTime updates the average execution time for a single observed execution.
func (mm *managerMetrics) consumeOneExecTime(execTime time.Duration) {
	avgExecTime := mm.averageExecTime.Load()
	taskExecutions := mm.totalTaskExecutions.Load()
	newAvgExecTime := (avgExecTime*time.Duration(taskExecutions) + execTime) /
		time.Duration(taskExecutions+1)
	mm.averageExecTime.Store(newAvgExecTime)
	mm.totalTaskExecutions.Add(1)
}

// updateTaskMetrics updates the task metrics. The input taskDelta is the number of tasks added or
// removed, and tasksPerSecond is the number of tasks executed per second by those tasks.
func (mm *managerMetrics) updateTaskMetrics(jobDelta, taskDelta int, taskCadence time.Duration) {
	// Calculate the new number of tasks in the queue
	currentTaskCount := mm.tasksManaged.Load()
	newTaskCount := currentTaskCount + int64(taskDelta)
	currentJobCount := mm.jobsManaged.Load()
	newJobCount := currentJobCount + int64(jobDelta)

	// Avoid division by zero for tasks; still update jobs count
	if newTaskCount <= 0 {
		mm.tasksPerSecond.Store(0)
		mm.tasksManaged.Store(0)
		mm.jobsManaged.Store(max(newJobCount, 0))
		return
	}

	// Calculate the new tasks per second
	tasksPerSecond := eventsPerSecond(taskDelta, taskCadence)
	jobsPerSecond := eventsPerSecond(jobDelta, taskCadence)

	// Update the tasks per second metric base on a weighted average
	newTasksPerSecond := (tasksPerSecond*float32(taskDelta) +
		mm.tasksPerSecond.Load()*float32(currentTaskCount)) /
		float32(newTaskCount)

	// Update the jobs per second metric base on a weighted average
	newJobsPerSecond := (jobsPerSecond*float32(jobDelta) +
		mm.jobsPerSecond.Load()*float32(currentJobCount)) /
		float32(newJobCount)

	// Store updated values
	mm.tasksPerSecond.Store(newTasksPerSecond)
	mm.tasksManaged.Add(int64(taskDelta))
	mm.jobsPerSecond.Store(newJobsPerSecond)
	mm.jobsManaged.Add(int64(jobDelta))
}

// eventsPerSecond calculates the number of events executed per second.
func eventsPerSecond(n int, cadence time.Duration) float32 {
	if cadence == 0 {
		return 0
	}
	events := math.Abs(float64(n))
	return float32(events) / float32(cadence.Seconds())
}
