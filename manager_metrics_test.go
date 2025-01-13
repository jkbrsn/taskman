package taskman

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestUpdateMetrics(t *testing.T) {
	doneChan := make(chan struct{})
	metrics := &managerMetrics{
		done: doneChan,
	}
	defer func() { close(doneChan) }()

	// Initial state
	initialTasksInQueue := metrics.tasksInQueue.Load()
	initialTasksPerSecond := metrics.tasksPerSecond.Load()

	// Update stats with tasks and cadence producing 2 tasks per second
	additionalTasks := 10
	cadence := 5 * time.Second
	metrics.updateTaskMetrics(additionalTasks, cadence)

	// Verify the tasksInQueue is updated correctly
	expectedTasksTotal := initialTasksInQueue + int64(additionalTasks)
	assert.Equal(t, expectedTasksTotal, metrics.tasksInQueue.Load(), "Expected tasksInQueue to be %d, got %d", expectedTasksTotal, metrics.tasksInQueue.Load())

	// Verify the tasksPerSecond is updated correctly
	expectedTasksPerSecond := initialTasksPerSecond + float32(additionalTasks)/float32(cadence.Seconds())
	assert.InDelta(t, expectedTasksPerSecond, metrics.tasksPerSecond.Load(), 0.001, "Expected tasksPerSecond to be %f, got %f", expectedTasksPerSecond, metrics.tasksPerSecond.Load())

	// Update stats with another set of tasks, this time producing 5 tasks per second
	additionalTasks = 10
	cadence = 2 * time.Second
	metrics.updateTaskMetrics(additionalTasks, cadence)

	// Verify that tasksInQueue is updated correctly
	expectedTasksTotal += int64(additionalTasks)
	assert.Equal(t, expectedTasksTotal, metrics.tasksInQueue.Load(), "Expected tasksInQueue to be %d, got %d", expectedTasksTotal, metrics.tasksInQueue.Load())

	// Verify that tasksPerSecond is updated correctly
	expectedTasksPerSecond = float32(2*10+5*10) / float32(20)
	assert.InDelta(t, expectedTasksPerSecond, metrics.tasksPerSecond.Load(), 0.001, "Expected tasksPerSecond to be %f, got %f", expectedTasksPerSecond, metrics.tasksPerSecond.Load())
}
