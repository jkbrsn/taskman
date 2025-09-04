package taskman

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestUpdateCadence(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	m := &executorMetrics{ctx: ctx}

	initialTasksPerSec := m.snapshot().TasksPerSecond
	initialJobsPerSec := m.snapshot().JobsPerSecond

	newTaskCount := 4
	old := 2 * time.Second
	newC := 1 * time.Second

	m.updateCadence(newTaskCount, old, newC)

	deltaTasks := float32(newTaskCount) * (1/float32(newC.Seconds()) - 1/float32(old.Seconds()))
	deltaJobs := (1/float32(newC.Seconds()) - 1/float32(old.Seconds()))

	assert.InDelta(t, initialTasksPerSec+deltaTasks, m.snapshot().TasksPerSecond, 1e-6)
	assert.InDelta(t, initialJobsPerSec+deltaJobs, m.snapshot().JobsPerSecond, 1e-6)

	// Reverse the cadence change; values should return to initial
	m.updateCadence(newTaskCount, newC, old)
	assert.InDelta(t, initialTasksPerSec, m.snapshot().TasksPerSecond, 1e-5)
	assert.InDelta(t, initialJobsPerSec, m.snapshot().JobsPerSecond, 1e-5)
}

func TestUpdateMetrics(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	metrics := &executorMetrics{
		ctx: ctx,
	}
	defer cancel()

	// Initial state
	initialTasksManaged := metrics.snapshot().TasksManaged
	initialTasksPerSecond := metrics.snapshot().TasksPerSecond

	// Update stats with tasks and cadence producing 2 tasks per second
	additionalTasks := 10
	cadence := 5 * time.Second
	metrics.updateMetrics(1, additionalTasks, cadence)

	// Verify the tasksInQueue is updated correctly
	expectedTasksTotal := initialTasksManaged + int64(additionalTasks)
	assert.Equal(
		t, expectedTasksTotal, metrics.snapshot().TasksManaged,
		"Expected tasksInQueue to be %d, got %d", expectedTasksTotal,
		metrics.snapshot().TasksManaged)

	// Verify the tasksPerSecond is updated correctly
	expectedTasksPerSecond := initialTasksPerSecond +
		float32(additionalTasks)/float32(cadence.Seconds())
	assert.InDelta(
		t, expectedTasksPerSecond, metrics.snapshot().TasksPerSecond,
		0.001, "Expected tasksPerSecond to be %f, got %f",
		expectedTasksPerSecond, metrics.snapshot().TasksPerSecond)

	// Update stats with another set of tasks, this time producing 5 tasks per second
	additionalTasks = 10
	cadence = 2 * time.Second
	metrics.updateMetrics(1, additionalTasks, cadence)

	// Verify that tasksInQueue is updated correctly
	expectedTasksTotal += int64(additionalTasks)
	assert.Equal(
		t, expectedTasksTotal, metrics.snapshot().TasksManaged,
		"Expected tasksInQueue to be %d, got %d",
		expectedTasksTotal, metrics.snapshot().TasksManaged)

	// Verify that tasksPerSecond is updated correctly
	expectedTasksPerSecond = initialTasksPerSecond + float32(10)/float32((5*time.Second).Seconds()) + float32(10)/float32((2*time.Second).Seconds())
	assert.InDelta(
		t, expectedTasksPerSecond, metrics.snapshot().TasksPerSecond,
		0.001, "Expected tasksPerSecond to be %f, got %f",
		expectedTasksPerSecond, metrics.snapshot().TasksPerSecond)
}
