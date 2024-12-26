package taskman

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewWorkerPool(t *testing.T) {
	resultChan := make(chan Result, 1)
	taskChan := make(chan Task, 1)
	pool := newWorkerPool(10, resultChan, taskChan)
	defer pool.stop()

	// Verify stopChan initialization
	assert.NotNil(t, pool.stopChan, "Expected stop channel to be non-nil")
}

func TestWorkerPoolStartStop(t *testing.T) {
	resultChan := make(chan Result, 1)
	taskChan := make(chan Task, 1)
	pool := newWorkerPool(4, resultChan, taskChan)
	defer func() {
		pool.stop()

		// Verify worker counts post-stop
		assert.Equal(t, 4, pool.workersTotal, "Expected worker count to be 4")
		assert.Equal(t, int32(0), pool.activeWorkers(), "Expected no active workers")
		assert.Equal(t, int32(0), pool.runningWorkers(), "Expected no running workers")
	}()

	// Verify worker counts pre-start
	assert.Equal(t, 4, pool.workersTotal, "Expected worker count to be 4")
	assert.Equal(t, int32(0), pool.activeWorkers(), "Expected no active workers")
	assert.Equal(t, int32(0), pool.runningWorkers(), "Expected no running workers")

	pool.start()
	time.Sleep(20 * time.Millisecond) // Wait for workers to start

	// Verify worker counts post-start
	assert.Equal(t, 4, pool.workersTotal, "Expected worker count to be 4")
	assert.Equal(t, int32(0), pool.activeWorkers(), "Expected no active workers")
	assert.Equal(t, int32(4), pool.runningWorkers(), "Expected 4 running workers")
}

func TestWorkerPoolTaskExecution(t *testing.T) {
	resultChan := make(chan Result, 1)
	taskChan := make(chan Task, 1)
	pool := newWorkerPool(1, resultChan, taskChan)
	defer pool.stop()

	// Start the worker
	pool.start()
	time.Sleep(10 * time.Millisecond) // Wait for worker to start

	// Create a task
	task := &MockTask{
		executeFunc: func() { time.Sleep(30 * time.Millisecond) },
		ID:          "test-task",
	}

	// Send the task to the worker and verify workers duringtask execution
	taskChan <- task
	time.Sleep(5 * time.Millisecond) // Wait for worker to pick up task
	assert.Equal(t, int32(1), pool.activeWorkers(), "Expected 1 active worker")

	// Verify workers after task execution
	time.Sleep(30 * time.Millisecond) // Wait for worker to execute task
	assert.Equal(t, int32(0), pool.activeWorkers(), "Expected 0 active workers")
}
