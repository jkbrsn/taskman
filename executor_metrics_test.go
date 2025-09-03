package taskman

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestUpdateMetrics(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	metrics := &executorMetrics{
		ctx: ctx,
	}
	defer cancel()

	// Initial state
	initialTasksManaged := metrics.tasksManaged.Load()
	initialTasksPerSecond := metrics.tasksPerSecond.Load()

	// Update stats with tasks and cadence producing 2 tasks per second
	additionalTasks := 10
	cadence := 5 * time.Second
	metrics.updateMetrics(1, additionalTasks, cadence)

	// Verify the tasksInQueue is updated correctly
	expectedTasksTotal := initialTasksManaged + int64(additionalTasks)
	assert.Equal(
		t, expectedTasksTotal, metrics.tasksManaged.Load(),
		"Expected tasksInQueue to be %d, got %d", expectedTasksTotal,
		metrics.tasksManaged.Load())

	// Verify the tasksPerSecond is updated correctly
	expectedTasksPerSecond := initialTasksPerSecond +
		float32(additionalTasks)/float32(cadence.Seconds())
	assert.InDelta(
		t, expectedTasksPerSecond, metrics.tasksPerSecond.Load(),
		0.001, "Expected tasksPerSecond to be %f, got %f",
		expectedTasksPerSecond, metrics.tasksPerSecond.Load())

	// Update stats with another set of tasks, this time producing 5 tasks per second
	additionalTasks = 10
	cadence = 2 * time.Second
	metrics.updateMetrics(1, additionalTasks, cadence)

	// Verify that tasksInQueue is updated correctly
	expectedTasksTotal += int64(additionalTasks)
	assert.Equal(
		t, expectedTasksTotal, metrics.tasksManaged.Load(),
		"Expected tasksInQueue to be %d, got %d",
		expectedTasksTotal, metrics.tasksManaged.Load())

	// Verify that tasksPerSecond is updated correctly
	expectedTasksPerSecond = float32(2*10+5*10) / float32(20)
	assert.InDelta(
		t, expectedTasksPerSecond, metrics.tasksPerSecond.Load(),
		0.001, "Expected tasksPerSecond to be %f, got %f",
		expectedTasksPerSecond, metrics.tasksPerSecond.Load())
}
