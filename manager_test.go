package taskman

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
)

var (
	testLogLevel = zerolog.InfoLevel
)

type MockTask struct {
	ID      string
	cadence time.Duration

	executeFunc func() error
}

func (mt MockTask) Execute() error {
	logger.Debug().Msgf("Executing MockTask with ID: %s", mt.ID)
	if mt.executeFunc != nil {
		err := mt.executeFunc()
		if err != nil {
			return err
		}
	}
	return nil
}

// Helper function to determine the buffer size of a channel
func getChannelBufferSize(ch any) int {
	switch v := ch.(type) {
	case chan Task:
		return cap(v)
	case chan error:
		return cap(v)
	default:
		return 0
	}
}

func setLoggerLevel(level zerolog.Level) {
	log.Logger = log.Output(zerolog.ConsoleWriter{
		Out:        zerolog.SyncWriter(os.Stderr), // Ensures thread safety
		TimeFormat: "15:04:05.999",
	}).Level(level)
}

// Helper function to get a Job with mocked tasks.
func getMockedJob(nTasks int, jobID string, cadence, timeToNextExec time.Duration) Job {
	var mockTasks []MockTask
	for i := 0; i < nTasks; i++ {
		mockTasks = append(mockTasks, MockTask{ID: fmt.Sprintf("task-%d", i), cadence: cadence})
	}
	// Explicitly convert []MockTask to []Task to satisfy the Job struct
	tasks := make([]Task, len(mockTasks))
	for i, task := range mockTasks {
		tasks[i] = task
	}
	job := Job{
		Cadence:  cadence,
		ID:       jobID,
		NextExec: time.Now().Add(timeToNextExec),
		Tasks:    tasks,
	}
	return job
}

func TestMain(m *testing.M) {
	zerolog.TimeFieldFormat = time.RFC3339Nano
	setLoggerLevel(testLogLevel)
	os.Exit(m.Run())
}

func TestNewTaskManagerCustom(t *testing.T) {
	manager := NewCustom(10, 1, 1*time.Minute)
	defer manager.Stop()

	// Verify jobQueue is initialized
	assert.NotNil(t, manager.jobQueue, "Expected job queue to be non-nil")

	// Verify taskChan is initialized and has the correct buffer size
	assert.NotNil(t, manager.taskChan, "Expected task channel to be non-nil")
	taskChanBuffer := getChannelBufferSize(manager.taskChan)
	assert.Equal(t, 1, taskChanBuffer, "Expected task channel to have buffer size 1")

	// Verify errorChan is initialized and has the correct buffer size
	assert.NotNil(t, manager.errorChan, "Expected error channel to be non-nil")
	errorChanBuffer := getChannelBufferSize(manager.errorChan)
	assert.Equal(t, 1, errorChanBuffer, "Expected error channel to have buffer size 1")
}

func TestManagerStop(t *testing.T) {
	// NewCustom starts the task manager
	manager := NewCustom(10, 2, 1*time.Minute)

	// Immediately stop the manager
	manager.Stop()

	// Attempt to add a task after stopping
	testChan := make(chan bool)
	testTask := MockTask{
		ID: "a-task", cadence: 50 * time.Millisecond, executeFunc: func() error {
			testChan <- true
			return nil
		},
	}
	_, err := manager.ScheduleTask(testTask, testTask.cadence)

	// Since the manager is stopped, the task should not have been added to the job queue
	assert.Error(t, err)
	if manager.jobsInQueue() != 0 {
		t.Fatalf("Expected job queue length to be 0, got %d", manager.jobsInQueue())
	}

	// Wait some time to see if the task executes
	select {
	case <-testChan:
		t.Fatal("Did not expect any tasks to be executed after TaskManager is stopped")
	case <-time.After(100 * time.Millisecond):
		// No tasks executed, as expected
	}
}

func TestScheduleFunc(t *testing.T) {
	manager := NewCustom(10, 2, 1*time.Minute)
	defer manager.Stop()

	jobID, err := manager.ScheduleFunc(
		func() error {
			return nil
		},
		100*time.Millisecond,
	)
	if err != nil {
		t.Fatalf("Error adding function: %v", err)
	}

	assert.Equal(t, 1, manager.jobsInQueue(),
		"Expected job queue length to be 1, got %d", manager.jobsInQueue())

	job := manager.jobQueue[0]
	assert.Equal(t, 1, len(job.Tasks), "Expected job to have 1 task, got %d", len(job.Tasks))
	assert.Equal(t, jobID, job.ID, "Expected job ID to be %s, got %s", jobID, job.ID)
}

func TestScheduleTask(t *testing.T) {
	manager := NewCustom(10, 2, 1*time.Minute)
	defer manager.Stop()

	testTask := MockTask{ID: "test-task", cadence: 100 * time.Millisecond}
	jobID, err := manager.ScheduleTask(testTask, testTask.cadence)
	if err != nil {
		t.Fatalf("Error adding task: %v", err)
	}

	assert.Equal(t, 1, manager.jobsInQueue(), "Expected job queue length to be 1, got %d",
		manager.jobsInQueue())

	job := manager.jobQueue[0]
	assert.Equal(t, 1, len(job.Tasks), "Expected job to have 1 task, got %d", len(job.Tasks))
	assert.Equal(t, testTask, job.Tasks[0], "Expected the task in the job to be the test task")
	assert.Equal(t, jobID, job.ID, "Expected job ID to be %s, got %s", jobID, job.ID)
}

func TestScheduleTasks(t *testing.T) {
	manager := NewCustom(10, 2, 1*time.Minute)
	defer manager.Stop()

	mockTasks := []MockTask{
		{ID: "task1", cadence: 100 * time.Millisecond},
		{ID: "task2", cadence: 100 * time.Millisecond},
	}
	// Explicitly convert []MockTask to []Task to satisfy the ScheduleTasks
	// method signature, since slices are not covariant in Go
	tasks := make([]Task, len(mockTasks))
	for i, task := range mockTasks {
		tasks[i] = task
	}
	jobID, err := manager.ScheduleTasks(tasks, 100*time.Millisecond)
	if err != nil {
		t.Fatalf("Error adding tasks: %v", err)
	}

	// Assert that the job was added
	assert.Equal(t, 1, manager.jobsInQueue(), "Expected job queue length to be 1, got %d",
		manager.jobsInQueue())
	job := manager.jobQueue[0]
	assert.Equal(t, 2, len(job.Tasks), "Expected job to have 2 tasks, got %d", len(job.Tasks))
	assert.Equal(t, jobID, job.ID, "Expected job ID to be %s, got %s", jobID, job.ID)
}

func TestScheduleJob(t *testing.T) {
	manager := NewCustom(10, 2, 1*time.Minute)
	defer manager.Stop()

	job := getMockedJob(2, "test-job", 100*time.Millisecond, 100*time.Millisecond)
	err := manager.ScheduleJob(job)
	if err != nil {
		t.Fatalf("Error adding job: %v", err)
	}

	// Assert that the job was added
	assert.Equal(t, 1, manager.jobsInQueue(), "Expected job queue length to be 1, got %d",
		manager.jobsInQueue())
	scheduledJob := manager.jobQueue[0]
	assert.Equal(t, len(job.Tasks), len(scheduledJob.Tasks),
		len(job.Tasks), "Expected job to have 2 tasks, got %d")
	assert.Equal(t, job.ID, scheduledJob.ID, "Expected job ID to be %s, got %s",
		scheduledJob.ID, job.ID)
}

func TestRemoveJob(t *testing.T) {
	manager := NewCustom(10, 2, 1*time.Minute)
	defer manager.Stop()

	job := getMockedJob(2, "someJob", 100*time.Millisecond, 100*time.Millisecond)
	err := manager.ScheduleJob(job)
	assert.Nil(t, err, "Error adding job")

	// Assert that the job was added
	assert.Equal(t, 1, manager.jobsInQueue(), "Expected job queue length to be 1, got %d",
		manager.jobsInQueue())
	qJob := manager.jobQueue[0]
	assert.Equal(t, job.ID, qJob.ID, "Expected job ID to be %s, got %s", job.ID, qJob.ID)
	assert.Equal(t, 2, len(qJob.Tasks), "Expected job to have 2 tasks, got %d", len(qJob.Tasks))

	// Remove the job
	err = manager.RemoveJob(job.ID)
	assert.Nil(t, err, "Error removing job")

	// Assert that the job was removed
	assert.Equal(t, 0, manager.jobsInQueue(), "Expected job queue length to be 0, got %d",
		manager.jobsInQueue())

	// Try removing the job once more
	err = manager.RemoveJob(job.ID)
	assert.Error(t, err, "Expected removal of non-existent job to produce an error")
}

func TestReplaceJob(t *testing.T) {
	manager := NewCustom(4, 4, 1*time.Minute)
	defer manager.Stop()

	// Add a job
	firstJob := getMockedJob(2, "aJobID", 100*time.Millisecond, 100*time.Millisecond)
	err := manager.ScheduleJob(firstJob)
	assert.Nil(t, err, "Error adding job")
	// Assert job added
	assert.Equal(t, 1, manager.jobsInQueue(), "Expected job queue length to be 1, got %d",
		manager.jobsInQueue())
	qJob := manager.jobQueue[0]
	assert.Equal(t, firstJob.ID, qJob.ID, "Expected ID to be '%s', got '%s'", firstJob.ID, qJob.ID)

	// Replace the first job
	secondJob := getMockedJob(4, "aJobID", 50*time.Millisecond, 100*time.Millisecond)
	err = manager.ReplaceJob(secondJob)
	assert.Nil(t, err, "Error replacing job")
	// Assert that the job was replaced in the queue
	assert.Equal(t, 1, manager.jobsInQueue(), "Expected job queue length to be 1, got %d",
		manager.jobsInQueue())
	qJob = manager.jobQueue[0]
	// The queue job should retain the index and NextExec time of the first job
	assert.Equal(t, firstJob.index, qJob.index, "Expected index to be '%s', got '%s'",
		secondJob.index, qJob.index)
	assert.Equal(t, firstJob.NextExec, qJob.NextExec, "Expected ID to be '%s', got '%s'",
		secondJob.NextExec, qJob.NextExec)
	// The queue job should have the ID, cadence and tasks of the new (second) job
	assert.Equal(t, secondJob.ID, qJob.ID, "Expected ID to be '%s', got '%s'",
		secondJob.ID, qJob.ID)
	assert.Equal(t, secondJob.Cadence, qJob.Cadence, "Expected cadence to be '%s', got '%s'",
		secondJob.Cadence, qJob.Cadence)
	assert.Equal(t, len(secondJob.Tasks), len(qJob.Tasks), "Expected job to have %d tasks, got %d",
		len(secondJob.Tasks), len(qJob.Tasks))

	// Try to replace a non-existing job
	thirdJob := getMockedJob(2, "anotherJobID", 10*time.Millisecond, 100*time.Millisecond)
	err = manager.ReplaceJob(thirdJob)
	assert.Error(t, err, "Expected replace attempt of non-existent job to produce an error")
}

func TestTaskExecution(t *testing.T) {
	manager := NewCustom(10, 1, 1*time.Minute)
	defer manager.Stop()

	var wg sync.WaitGroup
	wg.Add(1)

	// Use a channel to receive execution times
	executionTimes := make(chan time.Time, 1)

	// Create a test task
	testTask := &MockTask{
		ID:      "test-execution-task",
		cadence: 100 * time.Millisecond,
		executeFunc: func() error {
			logger.Debug().Msg("Executing TestTaskExecution task")
			executionTimes <- time.Now()
			wg.Done()
			return nil
		},
	}

	_, err := manager.ScheduleTask(testTask, testTask.cadence)
	assert.NoError(t, err)

	select {
	case execTime := <-executionTimes:
		elapsed := time.Since(execTime)
		assert.Greater(t, 150*time.Millisecond, elapsed,
			"Task executed after %v, expected around 100ms", elapsed)
	case <-time.After(200 * time.Millisecond):
		t.Fatal("Task did not execute in expected time")
	}
}

func TestTaskRescheduling(t *testing.T) {
	// Make room in buffered channel for multiple errors (4), since we're not
	// consuming them in this test and the error channel otherwise blocks the
	// workers from executing tasks
	manager := NewCustom(10, 4, 1*time.Minute)
	defer manager.Stop()

	var executionTimes []time.Time
	var mu sync.Mutex

	// Create a test task that records execution times
	mockTask := &MockTask{
		ID:      "test-rescheduling-task",
		cadence: 100 * time.Millisecond,
		executeFunc: func() error {
			mu.Lock()
			executionTimes = append(executionTimes, time.Now())
			mu.Unlock()
			return nil
		},
	}

	_, err := manager.ScheduleTask(mockTask, mockTask.cadence)
	assert.NoError(t, err)

	// Wait for the task to execute multiple times
	// Sleeing for 350ms should allow for about 3 executions
	time.Sleep(350 * time.Millisecond)

	mu.Lock()
	execCount := len(executionTimes)
	mu.Unlock()

	assert.LessOrEqual(t, 3, execCount, "Expected at least 3 executions, got %d", execCount)

	// Check that the executions occurred at roughly the correct intervals
	mu.Lock()
	for i := 1; i < len(executionTimes); i++ {
		diff := executionTimes[i].Sub(executionTimes[i-1])
		if diff < 90*time.Millisecond || diff > 110*time.Millisecond {
			t.Fatalf("Execution interval out of expected range: %v", diff)
		}
	}
	mu.Unlock()
}

func TestScheduleTaskDuringExecution(t *testing.T) {
	manager := NewCustom(10, 1, 1*time.Minute)
	defer manager.Stop()

	// Dedicated channels for task execution signals
	task1Executed := make(chan struct{})
	defer close(task1Executed)

	// Use a context to cancel the test
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	trackTasks := map[string]bool{
		"task1": false,
		"task2": false,
	}
	var mu sync.Mutex

	// Create test tasks
	testTask1 := &MockTask{
		ID:      "test-add-mid-execution-task-1",
		cadence: 15 * time.Millisecond,
		executeFunc: func() error {
			select {
			case <-ctx.Done():
				return nil
			default:
				task1Executed <- struct{}{}
				mu.Lock()
				trackTasks["task1"] = true
				mu.Unlock()
			}
			return nil
		},
	}
	testTask2 := &MockTask{
		ID:      "test-add-mid-execution-task-2",
		cadence: 10 * time.Millisecond,
		executeFunc: func() error {
			select {
			case <-ctx.Done():
				return nil
			default:
				mu.Lock()
				trackTasks["task2"] = true
				mu.Unlock()
			}
			return nil
		},
	}

	// Schedule first task
	_, err := manager.ScheduleTask(testTask1, testTask1.cadence)
	assert.NoError(t, err)
	start := time.Now()

	select {
	case <-task1Executed:
		// Task executed as expected
	case <-time.After(20 * time.Millisecond):
		t.Fatalf(
			"Task 1 did not execute within the expected time frame (40ms), %v",
			time.Since(start))
	}

	// Consume task1Executed to prevent it from blocking
	go func() {
		for range task1Executed {
			// Do nothing
		}
	}()

	// Schedule second task after we've had at least one execution of the first task
	_, err = manager.ScheduleTask(testTask2, testTask2.cadence)
	assert.NoError(t, err)

	// Sleep enough time to make sure both tasks have executed at least once
	time.Sleep(30 * time.Millisecond)

	cancel()
	manager.Stop()

	// Check that both tasks have executed
	mu.Lock()
	for msg, executed := range trackTasks {
		if !executed {
			t.Fatalf("Task '%s' did not execute, %v", msg, time.Since(start))
		}
	}
	mu.Unlock()
}

func TestConcurrentScheduleTask(t *testing.T) {
	// TODO: deactivate debug logs for this test? Using setLoggerLevel(zerolog.InfoLevel)
	// causes a race condition due to the logger being shared across tests

	manager := NewCustom(10, 1, 1*time.Minute)
	defer manager.Stop()

	var wg sync.WaitGroup
	numGoroutines := 20
	numTasksPerGoroutine := 250

	for id := range numGoroutines {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := range numTasksPerGoroutine {
				taskID := fmt.Sprintf("task-%d-%d", id, j)
				// Use a long cadence to avoid task execution before test ends,
				// as this changes the queue length
				task := MockTask{ID: taskID, cadence: 2 * time.Second}
				_, err := manager.ScheduleTask(task, task.cadence)
				assert.NoError(t, err, "Error adding task concurrently")
			}
		}(id)
	}

	wg.Wait()

	// Verify that all tasks are scheduled
	expectedTasks := numGoroutines * numTasksPerGoroutine
	assert.Equal(t, expectedTasks, manager.jobsInQueue(), "Expected job queue length to be %d, got %d",
		expectedTasks, manager.jobsInQueue())
}

func TestConcurrentScheduleJob(t *testing.T) {
	// TODO: deactivate debug logs for this test? Using setLoggerLevel(zerolog.InfoLevel)
	// causes a race condition due to the logger being shared across tests

	manager := NewCustom(10, 1, 1*time.Minute)
	defer manager.Stop()

	var wg sync.WaitGroup
	numGoroutines := 20
	numTasksPerGoroutine := 250

	for id := range numGoroutines {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := range numTasksPerGoroutine {
				jobID := fmt.Sprintf("task-%d-%d", id, j)
				// Use a long cadence to avoid job execution before test ends,
				// as this changes the queue length
				job := getMockedJob(1, jobID, 100*time.Millisecond, 2*time.Second)
				err := manager.ScheduleJob(job)
				assert.NoError(t, err, "Error adding job concurrently")
			}
		}(id)
	}

	wg.Wait()

	// Verify that all tasks are scheduled
	expectedTasks := numGoroutines * numTasksPerGoroutine
	assert.Equal(t, expectedTasks, manager.jobsInQueue(),
		"Expected job queue length to be %d, got %d", expectedTasks, manager.jobsInQueue())
}

func TestZeroCadenceTask(t *testing.T) {
	manager := NewCustom(10, 1, 1*time.Minute)
	defer manager.Stop()

	testChan := make(chan bool)
	testTask := MockTask{ID: "zero-cadence-task", cadence: 0, executeFunc: func() error {
		testChan <- true
		return nil
	}}
	_, err := manager.ScheduleTask(testTask, testTask.cadence)
	assert.Error(t, err, "Expected error adding task with zero cadence")

	// Expect the task to not execute
	select {
	case <-testChan:
		// Task executed, which is unexpected
		t.Fatal("Task with zero cadence should not execute")
	case <-time.After(50 * time.Millisecond):
		// After 50ms, the task would have executed if it was scheduled
		logger.Debug().Msg("Task with zero cadence never executed")
	}
}

func TestValidateJob(t *testing.T) {
	manager := NewCustom(10, 1, 1*time.Minute)
	defer manager.Stop()

	// Test case: valid job
	validJob := Job{
		ID:       "valid-job",
		Cadence:  100 * time.Millisecond,
		NextExec: time.Now().Add(100 * time.Millisecond),
		Tasks:    []Task{MockTask{ID: "task1"}},
	}
	err := manager.validateJob(validJob)
	assert.NoError(t, err, "Expected no error for valid job")

	// Test case: invalid job with zero cadence
	invalidJobZeroCadence := Job{
		ID:       "invalid-job-zero-cadence",
		Cadence:  0,
		NextExec: time.Now().Add(100 * time.Millisecond),
		Tasks:    []Task{MockTask{ID: "task1"}},
	}
	err = manager.validateJob(invalidJobZeroCadence)
	assert.Error(t, err, "Expected error for job with zero cadence")

	// Test case: invalid job with no tasks
	invalidJobNoTasks := Job{
		ID:       "invalid-job-no-tasks",
		Cadence:  100 * time.Millisecond,
		NextExec: time.Now().Add(100 * time.Millisecond),
		Tasks:    []Task{},
	}
	err = manager.validateJob(invalidJobNoTasks)
	assert.Error(t, err, "Expected error for job with no tasks")

	// Test case: invalid job with zero NextExec time
	invalidJobZeroNextExec := Job{
		ID:       "invalid-job-zero-next-exec",
		Cadence:  100 * time.Millisecond,
		NextExec: time.Time{},
		Tasks:    []Task{MockTask{ID: "task1"}},
	}
	err = manager.validateJob(invalidJobZeroNextExec)
	assert.Error(t, err, "Expected error for job with zero NextExec time")

	// Test case: existing job ID
	alreadyPresentJob := Job{
		ID:       "job-in-queue",
		Cadence:  100 * time.Millisecond,
		NextExec: time.Now().Add(100 * time.Millisecond),
		Tasks:    []Task{MockTask{ID: "task1"}},
	}
	assert.NoError(t, manager.ScheduleJob(alreadyPresentJob))

	duplicateJob := alreadyPresentJob
	err = manager.validateJob(duplicateJob)
	assert.Error(t, err, "Expected error for duplicate job ID")
}

func TestErrorChannelConsumption(t *testing.T) {
	manager := NewCustom(10, 2, 1*time.Minute)
	defer manager.Stop()

	// Send error to the error channel before attempting to consume it
	manager.errorChan <- errors.New("error 1")

	// Get the read-only error channel
	errCh := manager.ErrorChannel()
	assert.NotNil(t, errCh, "ErrorChannel should return a valid channel")

	// Send additional error
	manager.errorChan <- errors.New("error 2")

	// Consume errors from the ErrorChannel
	receivedErrors := []string{}
	timeout := time.After(10 * time.Millisecond)

Loop:
	for {
		select {
		case err, ok := <-errCh:
			if !ok {
				break Loop // Channel closed
			}
			receivedErrors = append(receivedErrors, err.Error())
		case <-timeout:
			break Loop // Avoid infinite loop in case of test failure
		}
	}

	// Validate that the correct errors were received after transition
	assert.Contains(t, receivedErrors, "error 1")
	assert.Contains(t, receivedErrors, "error 2")
}

func TestManagerMetrics(t *testing.T) {
	workerCount := 2
	manager := NewCustom(workerCount, 2, 1*time.Minute)
	defer manager.Stop()

	executionTime := 25 * time.Millisecond

	t.Run("Task execution", func(t *testing.T) {
		// Schedule a job with a task that takes 20ms to execute
		var wg sync.WaitGroup
		wg.Add(1)
		job := Job{
			ID:       "test-execution-task",
			Cadence:  1 * time.Second, // Set a long cadence to avoid more than 1 execution
			NextExec: time.Now(),
			Tasks: []Task{MockTask{ID: "task1", executeFunc: func() error {
				time.Sleep(executionTime)
				wg.Done()
				return nil
			}}},
		}
		assert.NoError(t, manager.ScheduleJob(job))

		// Wait for the task to execute and the metrics to be updated
		wg.Wait()
		time.Sleep(10 * time.Millisecond) // Allow time for metrics to be updated

		// Verify the metrics
		assert.Equal(t, int64(1), manager.metrics.totalTaskExecutions.Load(),
			"Expected 1 total task to have been counted")
		assert.GreaterOrEqual(t, manager.metrics.averageExecTime.Load(), executionTime, "Expected task execution time to be at least 10ms")
	})

	t.Run("Get Metrics", func(t *testing.T) {
		metrics := manager.Metrics()

		// Verify task execution metrics
		assert.Equal(t, 1, metrics.TasksTotalExecutions, "Expected 1 total task to have been counted")
		assert.GreaterOrEqual(t, metrics.TaskAverageExecTime, executionTime, "Expected task execution time to be at least %v", executionTime)
		assert.Greater(t, metrics.TasksPerSecond, float32(0), "Expected tasks per second to be greater than 0")

		// Verify worker pool metrics
		assert.Equal(t, 0, metrics.WorkersActive, "Expected 2 active workers")
		assert.Equal(t, workerCount, metrics.WorkersRunning, "Expected 2 running workers")
		assert.Equal(t, workerCount, metrics.WorkerCountTarget, "Expected worker count target to be 2")
		assert.GreaterOrEqual(t, metrics.WorkerUtilization, float32(0), "Expected worker utilization to be >= 0")
		assert.LessOrEqual(t, metrics.WorkerUtilization, float32(1), "Expected worker utilization to be <= 1")
		assert.Greater(t, metrics.WorkerScalingEvents, 0, "Expected at least 1 worker scaling event")

		// Verify job queue metrics
		assert.Equal(t, 1, metrics.QueuedJobs, "Expected 1 job in queue")
		assert.Equal(t, 1, metrics.QueuedTasks, "Expected 1 task in queue")
		assert.Equal(t, 1, metrics.QueueMaxJobWidth, "Expected max job width to be 1 task")
	})
}

func TestWorkerPoolScaling(t *testing.T) {
	// Start a manager with 1 worker
	manager := NewCustom(1, 4, 1*time.Minute)
	defer manager.Stop()

	// The first two test cases sets cadences and task execution duration to values producing a predetermined number of
	// needed workers. The third test case uses a larger number of tasks to force scaling up. The fourth test case uses
	// removes jobs to force scaling down.

	t.Run("ScaleUpBasedOnJobWidth", func(t *testing.T) {
		job := Job{
			ID:       "test-job-width-scaling",
			Cadence:  5 * time.Millisecond,
			NextExec: time.Now().Add(20 * time.Millisecond),
			Tasks: []Task{MockTask{ID: "task1", executeFunc: func() error {
				logger.Debug().Msg("Executing task1")
				time.Sleep(20 * time.Millisecond) // Simulate 20 ms execution time
				return nil
			}}, MockTask{ID: "task2", executeFunc: func() error {
				logger.Debug().Msg("Executing task2")
				time.Sleep(20 * time.Millisecond) // Simulate 20 ms execution time
				return nil
			}}},
		}
		err := manager.ScheduleJob(job)
		assert.Nil(t, err, "Expected no error scheduling job")
		time.Sleep(5 * time.Millisecond) // Allow time for job to be scheduled + worker pool to scale

		assert.Equal(t, manager.workerPool.targetWorkerCount(), int32(4), "Expected target worker count to be 2 x the job task count")
	})

	t.Run("ScaleUpBasedOnConcurrencyNeeds", func(t *testing.T) {
		initialTargetWorkerCount := manager.workerPool.targetWorkerCount()
		job := Job{
			ID:       "test-concurrency-need-scaling",
			Cadence:  5 * time.Millisecond, // Set a low cadence to force scaling up
			NextExec: time.Now().Add(10 * time.Millisecond),
			Tasks: []Task{MockTask{ID: "task3", executeFunc: func() error {
				logger.Debug().Msg("Executing task3")
				time.Sleep(20 * time.Millisecond) // Simulate 20 ms execution time
				return nil
			}}},
		}
		err := manager.ScheduleJob(job)
		assert.Nil(t, err, "Expected no error scheduling job")

		// Allow time for job to be scheduled + executed, then scale again
		time.Sleep(45 * time.Millisecond)
		manager.scaleWorkerPool(0)
		time.Sleep(5 * time.Millisecond) // Allow time for worker pool to scale

		// Check greater than rather than an exact number, as computation time may vary
		assert.Greater(t, manager.workerPool.targetWorkerCount(), initialTargetWorkerCount, "Expected target worker count to be greater than initial count")
	})

	t.Run("ScaleUpBasedOnImmediateNeed", func(t *testing.T) {
		runningWorkers := manager.workerPool.runningWorkers()
		availableWorkers := manager.workerPool.availableWorkers()

		job := Job{
			ID:       "test-immediate-need-scaling",
			Cadence:  25 * time.Millisecond,
			NextExec: time.Now().Add(100 * time.Millisecond),
			Tasks: []Task{
				MockTask{ID: "task4"},
				MockTask{ID: "task5"},
				MockTask{ID: "task6"},
				MockTask{ID: "task7"},
			},
		}
		err := manager.ScheduleJob(job)
		assert.Nil(t, err, "Expected no error scheduling job")

		// Allow time for job to be scheduled and scaling to take place
		time.Sleep(5 * time.Millisecond)

		// Expected worker count is 1.5 x (the current worker count + extra workers needed)
		// Note: we'll assert within a delta here, since the state of the worker pool variables may
		//       have changed since the time the pool was scaled.
		expectedWorkerCount := math.Ceil((float64(runningWorkers) + (4.0 - float64(availableWorkers))) * 1.5)
		assert.InDelta(t, expectedWorkerCount, manager.workerPool.targetWorkerCount(), 2.0, "Expected target worker count to be max 2.0 away from %d", expectedWorkerCount)
	})

	t.Run("ScaleDown", func(t *testing.T) {
		manager := NewCustom(1, 1, 1*time.Minute)
		defer manager.Stop()

		job := Job{
			ID:       "test-job-width-scaling",
			Cadence:  5 * time.Millisecond,
			NextExec: time.Now().Add(20 * time.Millisecond),
			Tasks: []Task{MockTask{ID: "task1", executeFunc: func() error {
				logger.Debug().Msg("Executing task1")
				time.Sleep(20 * time.Millisecond) // Simulate 20 ms execution time
				return nil
			}}, MockTask{ID: "task2", executeFunc: func() error {
				logger.Debug().Msg("Executing task2")
				time.Sleep(20 * time.Millisecond) // Simulate 20 ms execution time
				return nil
			}}},
		}
		err := manager.ScheduleJob(job)
		assert.Nil(t, err, "Expected no error scheduling job")
		time.Sleep(5 * time.Millisecond) // Allow time for job to be scheduled + worker pool to scale

		initialRunningWorkers := manager.workerPool.runningWorkers()
		initialTargetWorkerCount := manager.workerPool.targetWorkerCount()

		// Remove the job
		err = manager.RemoveJob("test-job-width-scaling")
		assert.Nil(t, err, "Expected no error removing job")
		time.Sleep(5 * time.Millisecond) // Allow time for job to be removed

		// Check that the worker pool has scaled down
		assert.Less(t, manager.workerPool.runningWorkers(), initialRunningWorkers, "Expected running worker count to be less than initial count")
		assert.Less(t, manager.workerPool.targetWorkerCount(), initialTargetWorkerCount, "Expected target worker count to be less than initial count")
	})

	t.Run("ScaleUpRespectsMaxWorkerCount", func(t *testing.T) {
		manager := NewCustom(1, 1, 1*time.Minute)
		defer manager.Stop()

		// Create a job that would require more workers than maxWorkerCount
		largeJob := Job{
			ID:       "test-max-worker-scaling",
			Cadence:  10 * time.Millisecond,
			NextExec: time.Now().Add(20 * time.Millisecond),
			Tasks:    make([]Task, maxWorkerCount+10), // More tasks than maxWorkerCount
		}
		for i := range largeJob.Tasks {
			largeJob.Tasks[i] = MockTask{ID: fmt.Sprintf("task%d", i)}
		}
		err := manager.ScheduleJob(largeJob)
		assert.NoError(t, err, "Expected no error scheduling job")

		// Allow time for job to be scheduled and scaling to take place
		time.Sleep(200 * time.Millisecond)

		// Verify that the worker count is capped at maxWorkerCount
		assert.Equal(t, int32(maxWorkerCount), manager.workerPool.targetWorkerCount(),
			"Expected target worker count to be capped at maxWorkerCount")
		assert.Equal(t, int32(maxWorkerCount), manager.workerPool.runningWorkers(),
			"Expected running worker count to be capped at maxWorkerCount")

		// Try to scale up further with another large job
		largeJob2 := Job{
			ID:       "test-max-worker-scaling-2",
			Cadence:  10 * time.Millisecond,
			NextExec: time.Now().Add(20 * time.Millisecond),
			Tasks:    make([]Task, maxWorkerCount+10),
		}
		err = manager.ScheduleJob(largeJob2)
		assert.NoError(t, err, "Expected no error scheduling job")

		// Allow time for scaling to take place
		time.Sleep(25 * time.Millisecond)

		// Verify that the worker count is still capped at maxWorkerCount
		assert.Equal(t, int32(maxWorkerCount), manager.workerPool.targetWorkerCount(),
			"Expected target worker count to remain capped at maxWorkerCount")
		assert.Equal(t, int32(maxWorkerCount), manager.workerPool.runningWorkers(),
			"Expected running worker count to remain capped at maxWorkerCount")
	})

	t.Run("RemoveAllJobs", func(t *testing.T) {
		manager := NewCustom(1, 1, 1*time.Minute)
		defer manager.Stop()

		job := Job{
			ID:       "test-job",
			Cadence:  10 * time.Millisecond,
			NextExec: time.Now().Add(20 * time.Millisecond),
			Tasks:    make([]Task, 10),
		}
		for i := range job.Tasks {
			job.Tasks[i] = MockTask{ID: fmt.Sprintf("task%d", i)}
		}
		err := manager.ScheduleJob(job)
		assert.NoError(t, err)

		// Allow time for job to be scheduled and scaling to take place
		time.Sleep(5 * time.Millisecond)

		// Remove the job
		err = manager.RemoveJob("test-job")
		assert.NoError(t, err, "Expected no error removing job")
		time.Sleep(150 * time.Millisecond) // Allow time for job removal

		// Check that the worker pool has scaled down
		assert.Equal(t, int32(manager.minWorkerCount), manager.workerPool.targetWorkerCount(), "Expected target worker count to be %d after removing all jobs", manager.minWorkerCount)
	})
}

func TestWorkerPoolPeriodicScaling(t *testing.T) {
	// Start a manager with 1 worker, and a scaling interval of 40ms. The scaling interval is set
	// to occur after the first job has executed at least once.
	manager := NewCustom(1, 4, 50*time.Millisecond)
	defer manager.Stop()

	// Add a job with 4 x longer execution than cadence, resulting in at least 4 workers being
	// needed although only one additional will be added from scaling based on the job width
	job := Job{
		ID:       "test-periodic-scaling",
		Cadence:  5 * time.Millisecond,
		NextExec: time.Now().Add(20 * time.Millisecond),
		Tasks: []Task{MockTask{ID: "task1", executeFunc: func() error {
			logger.Debug().Msg("Executing task1")
			time.Sleep(20 * time.Millisecond) // Simulate 20 ms execution time
			return nil
		}}},
	}
	err := manager.ScheduleJob(job)
	assert.Nil(t, err, "Expected no error scheduling job")
	time.Sleep(5 * time.Millisecond) // Allow time for job to be scheduled + worker pool to scale

	assert.Equal(t, manager.workerPool.targetWorkerCount(), int32(2), "Expected target worker count to be 2 x the job task count")

	time.Sleep(50 * time.Millisecond) // Allow time for periodic scaling to occur

	assert.GreaterOrEqual(t, manager.workerPool.targetWorkerCount(), int32(4), "Expected target worker count to be greater or equal than 4")
}

func TestTaskExecutionAt(t *testing.T) {
	manager := NewCustom(1, 2, 1*time.Minute)
	defer manager.Stop()

	t.Run("With NextExec as time.Now()", func(t *testing.T) {
		doneChan := make(chan struct{})
		job := Job{
			ID:       "test-instant-execution-1",
			Cadence:  1 * time.Second,
			NextExec: time.Now(),
			Tasks: []Task{MockTask{ID: "task1", executeFunc: func() error {
				close(doneChan)
				return nil
			}}},
		}
		assert.NoError(t, manager.ScheduleJob(job), "Expected no error scheduling job")
		defer func() { assert.NoError(t, manager.RemoveJob(job.ID)) }()

		// Expect the task to execute immediately
		select {
		case <-doneChan:
			// Task executed as expected
		case <-time.After(1 * time.Millisecond):
			t.Fatal("Task did not execute immediately")
		}
	})

	t.Run("With NextExec before time.Now(), but within Cadence", func(t *testing.T) {
		doneChan := make(chan struct{})
		job := Job{
			ID:       "test-instant-execution-2",
			Cadence:  1 * time.Second,
			NextExec: time.Now().Add(-500 * time.Millisecond),
			Tasks: []Task{MockTask{ID: "task1", executeFunc: func() error {
				close(doneChan)
				return nil
			}}},
		}
		assert.NoError(t, manager.ScheduleJob(job), "Expected no error scheduling job")
		defer func() { assert.NoError(t, manager.RemoveJob(job.ID)) }()

		// Expect the task to execute immediately
		select {
		case <-doneChan:
			// Task executed as expected
		case <-time.After(1 * time.Millisecond):
			t.Fatal("Task did not execute immediately")
		}
	})

	t.Run("With NextExec more than a Cadence before time.Now()", func(t *testing.T) {
		doneChan := make(chan struct{})
		job := Job{
			ID:       "test-instant-execution-3",
			Cadence:  1 * time.Second,
			NextExec: time.Now().Add(-2 * time.Second),
			Tasks: []Task{MockTask{ID: "task1", executeFunc: func() error {
				close(doneChan)
				return nil
			}}},
		}
		err := manager.ScheduleJob(job)
		assert.Error(t, err, "Expected error when scheduling job")
	})

	t.Run("With NextExec more than once Cadence after time.Now()", func(t *testing.T) {
		// Use a channel to receive execution times
		executionTimes := make(chan time.Time, 1)
		start := time.Now()
		execDelay := 25 * time.Millisecond
		job := Job{
			ID:       "test-instant-execution-4",
			Cadence:  10 * time.Millisecond,
			NextExec: time.Now().Add(execDelay),
			Tasks: []Task{MockTask{ID: "task1", executeFunc: func() error {
				executionTimes <- time.Now()
				return nil
			}}},
		}
		err := manager.ScheduleJob(job)
		assert.NoError(t, err, "Expected no error scheduling job")

		// Verify the task executed at the correct time
		execTime := <-executionTimes
		elapsed := execTime.Sub(start)
		assert.GreaterOrEqual(t, elapsed, execDelay, "Task executed after %v, expected around 25ms", elapsed)
	})
}

func TestGoroutineLeak(t *testing.T) {
	// Get initial goroutine count
	initialGoroutines := runtime.NumGoroutine()

	// Create a manager with periodic scaling
	manager := NewCustom(1, 4, 10*time.Millisecond)
	defer manager.Stop()

	// Schedule a job that runs for a while
	job := Job{
		ID:       "test-job",
		Cadence:  5 * time.Millisecond,
		NextExec: time.Now().Add(5 * time.Millisecond),
		Tasks: []Task{
			MockTask{
				ID: "test-task",
				executeFunc: func() error {
					time.Sleep(5 * time.Millisecond)
					return nil
				},
			},
		},
	}

	// Schedule the job
	err := manager.ScheduleJob(job)
	assert.NoError(t, err, "Expected no error scheduling job")

	// Run for a while to ensure periodic scaling and job execution occur
	time.Sleep(50 * time.Millisecond)

	// Stop the manager
	manager.Stop()

	// Wait for all goroutines to complete
	time.Sleep(25 * time.Millisecond)

	// Verify goroutine count is back to initial level
	finalGoroutines := runtime.NumGoroutine()
	// Allow for a small delta since other goroutines in the system may have started
	assert.InDelta(t, initialGoroutines, finalGoroutines, 5,
		"Expected goroutine count to return to initial level, got %d (initial: %d)",
		finalGoroutines, initialGoroutines)
}
