package taskman

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) {
	zerolog.TimeFieldFormat = time.RFC3339Nano
	m.Run()
}

var (
	// Activate and deactivate logs during tests using this variable
	testLogLevel = zerolog.Disabled // Disabled, DebugLevel, InfoLevel etc.
	testLogger   = zerolog.New(zerolog.NewTestWriter(nil)).Level(testLogLevel).
			Output(zerolog.ConsoleWriter{ // Ensures thread safety
			Out:        zerolog.SyncWriter(os.Stderr),
			TimeFormat: "15:04:05.999",
		})
)

type managerTestSuite struct {
	newManager func() *TaskManager
}

type MockTask struct {
	ID      string
	cadence time.Duration

	executeFunc func() error
}

func (mt MockTask) Execute() error {
	testLogger.Debug().Msgf("Executing MockTask with ID: %s", mt.ID)
	if mt.executeFunc != nil {
		return mt.executeFunc()
	}
	return nil
}

// Helper function to get a Job with mocked tasks.
func getMockedJob(nTasks int, jobID string, cadence, timeToNextExec time.Duration) Job {
	var mockTasks []MockTask
	for i := range nTasks {
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

// runManagerTestSuite runs all tests in the suite.
func runManagerTestSuite(t *testing.T, s *managerTestSuite) {
	t.Run("ManagerStop", s.TestManagerStop)
	t.Run("ScheduleFunc", s.TestScheduleFunc)
	t.Run("ScheduleTask", s.TestScheduleTask)
	t.Run("ScheduleTasks", s.TestScheduleTasks)
	t.Run("ScheduleJob", s.TestScheduleJob)
	t.Run("RemoveJob", s.TestRemoveJob)
	t.Run("ReplaceJob", s.TestReplaceJob)
	t.Run("TaskExecution", s.TestTaskExecution)
	t.Run("TaskRescheduling", s.TestTaskRescheduling)
	t.Run("ScheduleTaskDuringExecution", s.TestScheduleTaskDuringExecution)
	t.Run("ConcurrentScheduleTask", s.TestConcurrentScheduleTask)
	t.Run("ConcurrentScheduleJob", s.TestConcurrentScheduleJob)
	t.Run("ZeroCadenceTask", s.TestZeroCadenceTask)
	t.Run("ErrorChannelConsumption", s.TestErrorChannelConsumption)
	t.Run("ManagerMetrics", s.TestManagerMetrics)
	t.Run("TaskExecutionAt", s.TestTaskExecutionAt)
	t.Run("GoroutineLeak", s.TestGoroutineLeak)
}

func TestManager(t *testing.T) {
	t.Run("PoolExecutor", func(t *testing.T) {
		newManager := func() *TaskManager {
			return New(
				WithMode(ModePool),
				WithMPMinWorkerCount(10),
				WithChannelSize(2),
				WithMPScaleInterval(1*time.Minute))
		}
		runManagerTestSuite(t, &managerTestSuite{newManager: newManager})
	})

	t.Run("DistributedExecutor", func(t *testing.T) {
		newManager := func() *TaskManager {
			return New(
				WithMode(ModeDistributed),
				WithChannelSize(2))
		}
		runManagerTestSuite(t, &managerTestSuite{newManager: newManager})
	})
}

func (s *managerTestSuite) TestManagerStop(t *testing.T) {
	// NewCustom starts the task manager
	manager := s.newManager()

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
	// Assert no jobs in manager
	// TODO: implement

	// Wait some time to see if the task executes
	select {
	case <-testChan:
		t.Fatal("Did not expect any tasks to be executed after TaskManager is stopped")
	case <-time.After(100 * time.Millisecond):
		// No tasks executed, as expected
	}
}

func (s *managerTestSuite) TestScheduleFunc(t *testing.T) {
	manager := s.newManager()
	defer manager.Stop()

	_, err := manager.ScheduleFunc(
		func() error {
			return nil
		},
		100*time.Millisecond,
	)
	require.NoError(t, err)

	// Assert that the func was added
	assert.Equal(t, 1, manager.Metrics().ManagedJobs)
	assert.Equal(t, 1, manager.Metrics().ManagedTasks)
}

func (s *managerTestSuite) TestScheduleTask(t *testing.T) {
	manager := s.newManager()
	defer manager.Stop()

	testTask := MockTask{ID: "test-task", cadence: 100 * time.Millisecond}
	_, err := manager.ScheduleTask(testTask, testTask.cadence)
	require.NoError(t, err)

	// Assert that the task was added
	assert.Equal(t, 1, manager.Metrics().ManagedJobs)
	assert.Equal(t, 1, manager.Metrics().ManagedTasks)
}

func (s *managerTestSuite) TestScheduleTasks(t *testing.T) {
	manager := s.newManager()
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
	_, err := manager.ScheduleTasks(tasks, 100*time.Millisecond)
	require.NoError(t, err, "Error scheduling tasks")

	// Assert that the tasks were added
	assert.Equal(t, 1, manager.Metrics().ManagedJobs)
	assert.Equal(t, 2, manager.Metrics().ManagedTasks)
}

func (s *managerTestSuite) TestScheduleJob(t *testing.T) {
	manager := s.newManager()
	defer manager.Stop()

	job := getMockedJob(2, "test-job", 100*time.Millisecond, 100*time.Millisecond)

	t.Run("valid scheduling", func(t *testing.T) {
		assert.NoError(t, manager.ScheduleJob(job))
		// Assert that the job was added
		assert.Equal(t, 1, manager.Metrics().ManagedJobs)
		assert.Equal(t, 2, manager.Metrics().ManagedTasks)
	})

	t.Run("duplicate job", func(t *testing.T) {
		assert.Error(t, manager.ScheduleJob(job), "Expected error for duplicate job ID")
	})
}

func (s *managerTestSuite) TestRemoveJob(t *testing.T) {
	manager := s.newManager()
	defer manager.Stop()

	job := getMockedJob(2, "someJob", 100*time.Millisecond, 100*time.Millisecond)
	err := manager.ScheduleJob(job)
	assert.NoError(t, err, "Error adding job")

	// Assert that the job was added
	assert.Equal(t, 1, manager.Metrics().ManagedJobs)
	assert.Equal(t, 2, manager.Metrics().ManagedTasks)

	// Remove the job
	err = manager.RemoveJob(job.ID)
	assert.NoError(t, err, "Error removing job")

	// Assert that the job was removed
	assert.Equal(t, 0, manager.Metrics().ManagedJobs)
	assert.Equal(t, 0, manager.Metrics().ManagedTasks)

	// Try removing the job once more
	err = manager.RemoveJob(job.ID)
	assert.Error(t, err, "Expected removal of non-existent job to produce an error")
}

func (s *managerTestSuite) TestReplaceJob(t *testing.T) {
	manager := s.newManager()
	defer manager.Stop()

	// Add a job
	firstJob := getMockedJob(2, "aJobID", 100*time.Millisecond, 100*time.Millisecond)
	err := manager.ScheduleJob(firstJob)
	assert.NoError(t, err, "Error adding job")
	// Assert job added
	assert.Equal(t, 1, manager.Metrics().ManagedJobs)
	assert.Equal(t, 2, manager.Metrics().ManagedTasks)

	// Replace the first job
	secondJob := getMockedJob(4, "aJobID", 50*time.Millisecond, 100*time.Millisecond)
	err = manager.ReplaceJob(secondJob)
	assert.NoError(t, err, "Error replacing job")
	// Assert that the job was replaced in the queue
	assert.Equal(t, 1, manager.Metrics().ManagedJobs)
	assert.Equal(t, 4, manager.Metrics().ManagedTasks)
	// Check job properties
	job, err := manager.exec.Job("aJobID")
	assert.NoError(t, err)
	// The queue job should retain the index and NextExec time of the first job
	assert.Equal(t, firstJob.index, job.index)
	assert.Equal(t, firstJob.NextExec, job.NextExec)
	// The queue job should have the ID, cadence and tasks of the new (second) job
	assert.Equal(t, secondJob.ID, job.ID)
	assert.Equal(t, secondJob.Cadence, job.Cadence)
	assert.Equal(t, secondJob.Tasks, job.Tasks)

	// Try to replace a non-existing job
	thirdJob := getMockedJob(2, "anotherJobID", 10*time.Millisecond, 100*time.Millisecond)
	err = manager.ReplaceJob(thirdJob)
	assert.Error(t, err, "Expected replace attempt of non-existent job to produce an error")
}

func (s *managerTestSuite) TestTaskExecution(t *testing.T) {
	manager := s.newManager()
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
			testLogger.Debug().Msg("Executing TestTaskExecution task")
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

func (s *managerTestSuite) TestTaskRescheduling(t *testing.T) {
	// Make room in buffered channel for multiple errors (4), since we're not
	// consuming them in this test and the error channel otherwise blocks the
	// workers from executing tasks
	manager := s.newManager()
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

func (s *managerTestSuite) TestScheduleTaskDuringExecution(t *testing.T) {
	manager := s.newManager()
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
		for {
			select {
			case <-task1Executed:
				// drained
			case <-ctx.Done():
				return
			}
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

func (s *managerTestSuite) TestConcurrentScheduleTask(t *testing.T) {
	manager := s.newManager()
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
	// expectedTasks := numGoroutines * numTasksPerGoroutine
	// TODO: implement
}

func (s *managerTestSuite) TestConcurrentScheduleJob(t *testing.T) {
	manager := s.newManager()
	defer manager.Stop()

	var wg sync.WaitGroup
	numGoroutines := 20
	numJobsPerGoroutine := 250

	for id := range numGoroutines {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := range numJobsPerGoroutine {
				jobID := fmt.Sprintf("job-%d-%d", id, j)
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
	// expectedJobs := numGoroutines * numJobsPerGoroutine
	// TODO: implement
}

func (s *managerTestSuite) TestZeroCadenceTask(t *testing.T) {
	manager := s.newManager()
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
		testLogger.Debug().Msg("Task with zero cadence never executed")
	}
}

func (s *managerTestSuite) TestErrorChannelConsumption(t *testing.T) {
	manager := s.newManager()
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

func (*managerTestSuite) TestManagerMetrics(t *testing.T) {
	workerCount := 8
	// Create a manager with specific options for this test
	manager := New(
		WithLogger(testLogger),
		WithChannelSize(32),
		WithMPMinWorkerCount(workerCount),
		WithMPScaleInterval(1*time.Minute)) // Set a long interval to avoid scaling during test
	defer manager.Stop()

	executionTime := 10 * time.Millisecond
	cadence := 10 * time.Millisecond
	taskCount := 2

	// Schedule a job with a task that takes 10ms to execute, and a cadence of 25ms
	job := Job{
		ID:       "test-execution-task",
		Cadence:  cadence,
		NextExec: time.Now(),
		Tasks:    make([]Task, taskCount),
	}
	for i := range taskCount {
		job.Tasks[i] = MockTask{ID: fmt.Sprintf("task%d", i), executeFunc: func() error {
			time.Sleep(executionTime)
			return nil
		}}
	}
	assert.NoError(t, manager.ScheduleJob(job))

	// Wait for at least 4 job executions, so 8 task executions
	const expectedExecutions = 4
	time.Sleep(cadence*expectedExecutions + executionTime)
	metrics := manager.Metrics()

	// Verify job queue metrics
	assert.Equal(t, 1, metrics.ManagedJobs, "Expected 1 job in queue")
	assert.Equal(t, taskCount, metrics.ManagedTasks, "Expected %d task in queue", taskCount)

	// Verify task execution metrics
	assert.GreaterOrEqual(t, metrics.JobsTotalExecutions, expectedExecutions,
		"Expected at least %d total jobs to have been counted", expectedExecutions)
	assert.GreaterOrEqual(t, metrics.TasksTotalExecutions, expectedExecutions*taskCount,
		"Expected at least %d total task to have been counted", expectedExecutions*taskCount)
	assert.GreaterOrEqual(t, metrics.TasksAverageExecTime, executionTime,
		"Expected task execution time to be at least %v", executionTime)
	assert.InDelta(t, 1/cadence.Seconds(), metrics.JobsPerSecond, 0.1,
		"Expected jobs per second to be around %f", 1/cadence.Seconds())
	assert.InDelta(t, float64(taskCount)/cadence.Seconds(), metrics.TasksPerSecond, 0.1,
		"Expected tasks per second to be around %f", float64(taskCount)/cadence.Seconds())

	// Verify worker pool metrics
	assert.GreaterOrEqual(t, metrics.PoolMetrics.WorkersActive, 0,
		"Expected active workers to be >= 0")
	assert.Equal(t, workerCount, metrics.PoolMetrics.WorkersRunning,
		"Expected %d running workers", workerCount)
	assert.Equal(t, workerCount, metrics.PoolMetrics.WorkerCountTarget,
		"Expected worker count target to be %d", workerCount)
	assert.GreaterOrEqual(t, metrics.PoolMetrics.WorkerUtilization, float32(0),
		"Expected worker utilization to be >= 0")
	assert.LessOrEqual(t, metrics.PoolMetrics.WorkerUtilization, float32(1),
		"Expected worker utilization to be <= 1")
	assert.GreaterOrEqual(t, metrics.PoolMetrics.WorkerScalingEvents, 1,
		"Expected at least 1 worker scaling event")
	assert.Equal(t, taskCount, metrics.PoolMetrics.WidestJobWidth,
		"Expected max job width to be %d task", taskCount)
}

func (s *managerTestSuite) TestTaskExecutionAt(t *testing.T) {
	manager := s.newManager()
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
		assert.GreaterOrEqual(
			t,
			elapsed,
			execDelay,
			"Task executed after %v, expected around 25ms",
			elapsed,
		)
	})
}

func (*managerTestSuite) TestGoroutineLeak(t *testing.T) {
	// Get initial goroutine count
	initialGoroutines := runtime.NumGoroutine()

	// Create a manager with periodic scaling
	manager := New(
		WithMPMinWorkerCount(1),
		WithChannelSize(4),
		WithMPScaleInterval(10*time.Millisecond),
	)
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
