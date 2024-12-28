package taskman

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewWorkerPool(t *testing.T) {
	errorChan := make(chan error, 1)
	taskChan := make(chan Task, 1)
	workerPoolDone := make(chan struct{})
	pool := newWorkerPool(10, errorChan, taskChan, workerPoolDone)
	defer pool.stop()

	// Verify stopChan initialization
	assert.NotNil(t, pool.stopChan, "Expected stop channel to be non-nil")
}

func TestWorkerPoolStartStop(t *testing.T) {
	errorChan := make(chan error, 1)
	taskChan := make(chan Task, 1)
	workerPoolDone := make(chan struct{})
	pool := newWorkerPool(4, errorChan, taskChan, workerPoolDone)
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
	errorChan := make(chan error, 1)
	taskChan := make(chan Task, 1)
	workerPoolDone := make(chan struct{})
	pool := newWorkerPool(1, errorChan, taskChan, workerPoolDone)
	defer pool.stop()

	// Start the worker
	pool.start()
	time.Sleep(10 * time.Millisecond) // Wait for worker to start

	// Create a task
	task := &MockTask{
		executeFunc: func() error {
			time.Sleep(30 * time.Millisecond)
			return nil
		},
		ID: "test-task",
	}

	// Send the task to the worker and verify active workers during task execution
	taskChan <- task
	time.Sleep(5 * time.Millisecond) // Wait for worker to pick up task
	assert.Equal(t, int32(1), pool.activeWorkers(), "Expected 1 active worker")

	// Verify workers after task execution
	time.Sleep(30 * time.Millisecond) // Wait for worker to execute task
	assert.Equal(t, int32(0), pool.activeWorkers(), "Expected 0 active workers")
}

func TestWorkerPoolExecutionError(t *testing.T) {
	errorChan := make(chan error, 1)
	taskChan := make(chan Task, 1)
	workerPoolDone := make(chan struct{})
	pool := newWorkerPool(1, errorChan, taskChan, workerPoolDone)
	defer pool.stop()

	// Start the worker
	pool.start()
	time.Sleep(10 * time.Millisecond) // Wait for worker to start

	// Create a task which produces an error
	errorTask := &MockTask{
		executeFunc: func() error {
			return errors.New("test error")
		},
		ID: "error-task",
	}

	// Listen to the error channel, confirm error is received
	timeout := time.After(50 * time.Millisecond)
	go func() {
	Loop:
		for {
			select {
			case err, ok := <-errorChan:
				if !ok {
					break Loop // Channel closed
				}
				assert.Contains(t, err.Error(), "test error")
			case <-timeout:
				assert.Fail(t, "Test timed out waiting on error")
				break Loop // Avoid infinite loop in case of test failure
			}
		}
	}()

	// Send the error-returning task to the worker
	taskChan <- errorTask
	time.Sleep(10 * time.Millisecond) // Sleep to let the task propagate

	// TODO: test execution with nil error
}
