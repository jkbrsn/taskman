package taskman

import (
	"math"
	"sync/atomic"
	"time"

	uatomic "go.uber.org/atomic"
)

// TaskManagerMetrics stores metrics about the task manager.
type TaskManagerMetrics struct {
	// Jobs
	ManagedJobs int // Total number of jobs in the queue

	// Task execution
	ManagedTasks         int           // Total number of tasks in the queue
	TaskAverageExecTime  time.Duration // Average execution time of tasks
	TasksTotalExecutions int           // Total number of tasks executed
	TasksPerSecond       float32       // Number of tasks executed per second

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

// PoolMetrics stores metrics about the worker pool.
type PoolMetrics struct {
	WidestJobWidth int // Widest job in the queue in terms of number of tasks

	// Worker pool
	WorkerCountTarget   int     // Target number of workers
	WorkerScalingEvents int     // Number of worker scaling events since start
	WorkerUtilization   float32 // Utilization of workers
	WorkersActive       int     // Number of active workers
	WorkersRunning      int     // Number of running workers
}

// managerMetrics stores internal metrics about the task manager.
type managerMetrics struct {
	// Task execution
	averageExecTime     uatomic.Duration // Average execution time of tasks
	totalTaskExecutions atomic.Int64     // Total number of tasks executed
	tasksPerSecond      uatomic.Float32  // Number of tasks executed per second

	// Task management
	tasksManaged atomic.Int64 // Total number of tasks managed
	jobsManaged  atomic.Int64 // Total number of jobs managed

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
	currentJobCount := mm.jobsManaged.Load()
	newJobCount := currentJobCount + int64(jobDelta)
	currentTaskCount := mm.tasksManaged.Load()
	newTaskCount := currentTaskCount + int64(taskDelta)

	// Avoid division by zero for tasks; still update jobs count
	if newTaskCount <= 0 {
		mm.tasksPerSecond.Store(0)
		mm.tasksManaged.Store(0)
		mm.jobsManaged.Store(max(newJobCount, 0))
		return
	}

	// Calculate the new tasks per second
	tasksPerSecond := calcTasksPerSecond(taskDelta, taskCadence)

	// Update the tasks per second metric base on a weighted average
	newTasksPerSecond := (tasksPerSecond*float32(taskDelta) +
		mm.tasksPerSecond.Load()*float32(currentTaskCount)) /
		float32(newTaskCount)

	// Store updated values
	mm.tasksPerSecond.Store(newTasksPerSecond)
	mm.jobsManaged.Store(newJobCount)
	mm.tasksManaged.Store(newTaskCount)
}

// calcTasksPerSecond calculates the number of tasks executed per second.
func calcTasksPerSecond(nTasks int, cadence time.Duration) float32 {
	if cadence == 0 {
		return 0
	}
	tasks := math.Abs(float64(nTasks))
	return float32(tasks) / float32(cadence.Seconds())
}
