package taskman

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func getWorkerPool(nWorkers int) *workerPool {
	errorChan := make(chan error, 1)
	taskExecChan := make(chan time.Duration, 1)
	taskChan := make(chan Task, 1)
	workerPoolDone := make(chan struct{})
	return newWorkerPool(testLogger, nWorkers, errorChan, taskExecChan, taskChan, workerPoolDone)
}

func TestNewWorkerPool(t *testing.T) {
	pool := getWorkerPool(10)
	defer pool.stop()

	// Verify stopPoolChan initialization
	assert.NotNil(t, pool.stopPoolChan, "Expected stop channel to be non-nil")
}

func TestWorkerPoolStartStop(t *testing.T) {
	pool := getWorkerPool(4)
	defer func() {
		pool.stop()

		// Verify worker counts post-stop
		assert.Equal(t, int32(0), pool.activeWorkers(), "Expected no active workers")
		assert.Equal(t, int32(0), pool.runningWorkers(), "Expected no running workers")
	}()

	time.Sleep(10 * time.Millisecond) // Wait for workers to start

	// Verify worker counts post-start
	assert.Equal(t, int32(0), pool.activeWorkers(), "Expected no active workers")
	assert.Equal(t, int32(4), pool.runningWorkers(), "Expected 4 running workers")
}

func TestWorkerPoolTaskExecution(t *testing.T) {
	errorChan := make(chan error, 1)
	taskExecChan := make(chan time.Duration, 1)
	taskChan := make(chan Task, 1)
	workerPoolDone := make(chan struct{})
	pool := newWorkerPool(testLogger, 1, errorChan, taskExecChan, taskChan, workerPoolDone)
	defer pool.stop()

	time.Sleep(10 * time.Millisecond) // Wait for worker to start

	// Create a task
	task := &MockTask{
		executeFunc: func() error {
			time.Sleep(30 * time.Millisecond)
			return nil
		},
		ID: "test-task",
	}

	// Listen to the error channel, confirm no error is received
	timeout := time.After(100 * time.Millisecond) // Timeout to close goroutine
	go func() {
		select {
		case err := <-errorChan:
			assert.Failf(t, "No error should have been received", err.Error())
		case <-timeout:
			return
		}
	}()

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
	taskExecChan := make(chan time.Duration, 1)
	taskChan := make(chan Task, 1)
	workerPoolDone := make(chan struct{})
	pool := newWorkerPool(testLogger, 1, errorChan, taskExecChan, taskChan, workerPoolDone)
	defer pool.stop()

	time.Sleep(10 * time.Millisecond) // Wait for worker to start

	// Create a task which produces an error
	errorTask := &MockTask{
		executeFunc: func() error {
			return errors.New("test error")
		},
		ID: "error-task",
	}

	var wg sync.WaitGroup
	wg.Add(1)

	// Listen to the error channel, confirm error is received
	timeout := time.After(100 * time.Millisecond)
	go func() {
		defer wg.Done()
		select {
		case err := <-errorChan:
			assert.Contains(t, err.Error(), "test error")
		case <-timeout:
			assert.Fail(t, "Test timed out waiting on error")
		}
	}()

	// Send the error-returning task to the worker
	taskChan <- errorTask
	wg.Wait() // Don't exit the test until the error has been received
}

func TestWorkerPoolExecutionPanic(t *testing.T) {
	errorChan := make(chan error, 1)
	taskExecChan := make(chan time.Duration, 1)
	taskChan := make(chan Task, 1)
	workerPoolDone := make(chan struct{})
	pool := newWorkerPool(testLogger, 1, errorChan, taskExecChan, taskChan, workerPoolDone)
	defer pool.stop()

	time.Sleep(5 * time.Millisecond) // Wait for worker to start

	// Create a task which panics
	panicTask := &MockTask{
		executeFunc: func() error {
			panic("test panic")
		},
		ID: "panic-task",
	}

	var wg sync.WaitGroup
	wg.Add(1)

	// Listen to the error channel, confirm error is received
	timeout := time.After(100 * time.Millisecond)
	go func() {
		defer wg.Done()
		select {
		case err := <-errorChan:
			assert.Contains(t, err.Error(), "panic:")
			assert.Contains(t, err.Error(), "test panic")
		case <-timeout:
			assert.Fail(t, "Test timed out waiting on error")
		}
	}()

	// Send the panic-returning task to the worker
	taskChan <- panicTask
	wg.Wait() // Don't exit the test until the error has been received
}

func TestWorkerPoolBusyWorkers(t *testing.T) {
	errorChan := make(chan error, 1)
	taskExecChan := make(chan time.Duration, 1)
	taskChan := make(chan Task, 1)
	workerPoolDone := make(chan struct{})
	pool := newWorkerPool(testLogger, 2, errorChan, taskExecChan, taskChan, workerPoolDone)
	defer pool.stop()

	time.Sleep(10 * time.Millisecond) // Wait for workers to start

	// Create tasks that will keep workers busy
	task1 := &MockTask{
		executeFunc: func() error {
			time.Sleep(50 * time.Millisecond)
			return nil
		},
		ID: "task-1",
	}
	task2 := &MockTask{
		executeFunc: func() error {
			time.Sleep(50 * time.Millisecond)
			return nil
		},
		ID: "task-2",
	}

	// Send tasks to the workers
	taskChan <- task1
	taskChan <- task2
	time.Sleep(5 * time.Millisecond) // Wait for workers to pick up tasks

	// Verify active workers during task execution
	assert.Equal(t, int32(2), pool.activeWorkers(), "Expected 2 active workers")

	// Create another task to be queued
	task3 := &MockTask{
		executeFunc: func() error {
			time.Sleep(50 * time.Millisecond)
			return nil
		},
		ID: "task-3",
	}

	// Send the third task while workers are busy
	taskChan <- task3
	time.Sleep(5 * time.Millisecond) // Allow some time for task to be queued

	// Verify that the third task is queued and not yet executed
	assert.Equal(t, int32(2), pool.activeWorkers(), "Expected 2 active workers")
	assert.Equal(t, 1, len(taskChan), "Expected 1 task in the queue")

	// Wait for the first two tasks to complete
	time.Sleep(50 * time.Millisecond)

	// Verify that the third task is now being executed
	assert.Equal(t, int32(1), pool.activeWorkers(), "Expected 1 active worker")
	assert.Equal(t, 0, len(taskChan), "Expected no tasks in the queue")
}

func TestStopWorker(t *testing.T) {
	errorChan := make(chan error, 1)
	taskExecChan := make(chan time.Duration, 1)
	taskChan := make(chan Task, 1)
	workerPoolDone := make(chan struct{})
	pool := newWorkerPool(testLogger, 2, errorChan, taskExecChan, taskChan, workerPoolDone)
	defer pool.stop()

	time.Sleep(10 * time.Millisecond) // Wait for workers to start

	// Confirm running workers
	assert.Equal(t, int32(2), pool.runningWorkers(), "Expected 2 running workers")

	// Get idle workers
	idleWorkers := pool.idleWorkers()
	assert.Equal(t, 2, len(idleWorkers), "Expected 2 idle workers")

	// Stop one of the workers
	assert.NoError(t, pool.stopWorker(idleWorkers[0]))
	time.Sleep(10 * time.Millisecond) // Wait for worker to stop

	// Confirm running workers after stopping one
	assert.Equal(t, int32(1), pool.runningWorkers(), "Expected 1 running worker")
	assert.Equal(t, 1, len(pool.idleWorkers()), "Expected 1 idle worker")

	// Add a task to the remaining worker that will keep it busy
	task := &MockTask{
		executeFunc: func() error {
			time.Sleep(20 * time.Millisecond)
			return nil
		},
		ID: "task-1",
	}
	taskChan <- task
	time.Sleep(5 * time.Millisecond) // Wait for worker to pick up task

	// Verify active/busy workers during task execution
	assert.Equal(t, int32(1), pool.activeWorkers(), "Expected 1 active worker")
	assert.Equal(t, 1, len(pool.busyWorkers()), "Expected 1 busy worker")

	// Stop the remaining worker
	assert.NoError(t, pool.stopWorker(idleWorkers[1]))
	time.Sleep(5 * time.Millisecond) // Wait for stop to take effect

	// Confirm worker hasn't stopped during execution
	assert.Equal(t, int32(1), pool.runningWorkers(), "Expected 1 running workers")
	activeWorkerAny, ok := pool.workers.Load(idleWorkers[1])
	assert.True(t, ok, "Expected worker to be found")
	activeWorkerTyped, ok := activeWorkerAny.(*workerInfo)
	assert.True(t, ok, "Expected worker to be of type *workerInfo")
	assert.True(t, activeWorkerTyped.busy.Load(), "Expected worker to be busy")

	// Verify worker stops after executing task
	time.Sleep(20 * time.Millisecond) // Wait for worker to execute task
	assert.Equal(t, int32(0), pool.runningWorkers(), "Expected 0 running workers")

	// Ensure no errors were reported
	select {
	case err := <-errorChan:
		t.Fatalf("Unexpected error: %v", err)
	default:
		// No error
	}
}

func TestStopWorkers(t *testing.T) {
	errorChan := make(chan error, 1)
	taskExecChan := make(chan time.Duration, 1)
	taskChan := make(chan Task, 1)
	workerPoolDone := make(chan struct{})
	pool := newWorkerPool(testLogger, 6, errorChan, taskExecChan, taskChan, workerPoolDone)
	defer pool.stop()

	time.Sleep(10 * time.Millisecond) // Wait for workers to start

	// Confirm worker counts
	assert.Equal(t, 6, len(pool.idleWorkers()), "Expected 6 idle workers")
	assert.Equal(t, int32(6), pool.runningWorkers(), "Expected 6 running workers")

	// Stop two workers
	assert.NoError(t, pool.stopWorkers(2))
	time.Sleep(10 * time.Millisecond) // Wait for workers to stop

	// Confirm worker counts after stopping two workers
	assert.Equal(t, 4, len(pool.idleWorkers()), "Expected 4 idle workers")
	assert.Equal(t, int32(4), pool.runningWorkers(), "Expected 4 running workers")

	// Synchronize task completions with a channel
	taskCompleted := make(chan struct{})
	// Add three tasks for execution, to make 3/4 workers busy
	task := &MockTask{
		executeFunc: func() error {
			time.Sleep(10 * time.Millisecond) // Simulate task execution
			taskCompleted <- struct{}{}
			return nil
		},
		ID: "task-1",
	}
	for range 3 {
		taskChan <- task
	}

	time.Sleep(5 * time.Millisecond) // Wait for workers to pick up tasks

	// Verify active workers during task execution
	assert.Equal(t, 1, len(pool.idleWorkers()), "Expected 1 idle workers")
	assert.Equal(t, 3, len(pool.busyWorkers()), "Expected 3 busy workers")
	assert.Equal(t, int32(3), pool.activeWorkers(), "Expected 3 active workers")

	// Stop more workers than there are idle workers available
	assert.NoError(t, pool.stopWorkers(2))
	time.Sleep(5 * time.Millisecond) // Wait for idle worker to stop

	// Confirm worker counts again, only 1 worker should have been be stopped
	assert.Equal(t, 0, len(pool.idleWorkers()), "Expected 0 idle workers")
	assert.Equal(t, 3, len(pool.busyWorkers()), "Expected 3 busy workers")
	assert.Equal(t, int32(3), pool.activeWorkers(), "Expected 3 active workers")
	assert.Equal(t, int32(3), pool.runningWorkers(), "Expected 3 running workers")

	// Wait for tasks to complete, which should free up the workers to be stopped
	for range 3 {
		<-taskCompleted
	}
	time.Sleep(5 * time.Millisecond)

	// Confirm one of the previously busy workers has stopped
	assert.Equal(t, 2, len(pool.idleWorkers()), "Expected 2 idle workers")
	assert.Equal(t, int32(2), pool.runningWorkers(), "Expected 2 running workers")

	// Stop more workers than there are workers in the pool
	assert.Error(t, pool.stopWorkers(10))

	// Confirm no workers were stopped
	assert.Equal(t, 2, len(pool.idleWorkers()), "Expected 2 idle workers")
	assert.Equal(t, int32(2), pool.runningWorkers(), "Expected 2 running workers")
}

func TestWorkerPoolUtilization(t *testing.T) {
	errorChan := make(chan error, 1)
	taskExecChan := make(chan time.Duration, 1)
	taskChan := make(chan Task, 1)
	workerPoolDone := make(chan struct{})
	pool := newWorkerPool(testLogger, 4, errorChan, taskExecChan, taskChan, workerPoolDone)
	defer pool.stop()

	time.Sleep(5 * time.Millisecond) // Wait for workers to start

	// Verify initial utilization
	assert.Equal(t, 0.0, pool.utilization(), "Expected initial utilization to be 0.0")

	// Create tasks that will keep workers busy
	task1 := &MockTask{
		executeFunc: func() error {
			time.Sleep(20 * time.Millisecond)
			return nil
		},
		ID: "task-1",
	}
	task2 := task1
	task2.ID = "task-2"

	// Send tasks to the workers
	taskChan <- task1
	taskChan <- task2
	time.Sleep(5 * time.Millisecond) // Wait for workers to pick up tasks

	// Verify utilization during task execution
	assert.Equal(t, 0.5, pool.utilization(), "Expected utilization to be 0.5")

	// Create another task to be queued
	task3 := &MockTask{
		executeFunc: func() error {
			time.Sleep(30 * time.Millisecond)
			return nil
		},
		ID: "task-3",
	}

	// Send the third task while workers are busy
	taskChan <- task3
	time.Sleep(5 * time.Millisecond) // Allow some time for task to be queued

	// Verify utilization remains the same as no new worker has picked up the task yet
	assert.Equal(t, 0.75, pool.utilization(), "Expected utilization to remain 0.5")

	// Wait for the first two tasks to complete
	time.Sleep(20 * time.Millisecond)

	// Verify utilization after first tasks have completed
	assert.Equal(t, 0.25, pool.utilization(), "Expected utilization to be 0.25")

	// Wait for the third task to complete
	time.Sleep(20 * time.Millisecond)

	// Verify utilization after all tasks are done
	assert.Equal(t, 0.0, pool.utilization(), "Expected utilization to be 0.0")
}
