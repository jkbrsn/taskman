package taskman

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
)

type MockTask struct {
	ID      string
	cadence time.Duration

	executeFunc func()
}

func (mt MockTask) Execute() Result {
	log.Debug().Msgf("Executing MockTask with ID: %s", mt.ID)
	if mt.executeFunc != nil {
		mt.executeFunc()
	}
	return Result{Success: true}
}

// Helper function to determine the buffer size of a channel
func getChannelBufferSize(ch interface{}) int {
	switch v := ch.(type) {
	case chan Task:
		return cap(v)
	case chan Result:
		return cap(v)
	default:
		return 0
	}
}

// TODO: write test comparing what happens when different channel types are used for taskChan

func TestMain(m *testing.M) {
	zerolog.TimeFieldFormat = time.RFC3339Nano
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: "15:04:05.999"}).Level(zerolog.DebugLevel)
	os.Exit(m.Run())
}

func TestNewScheduler(t *testing.T) {
	scheduler := NewScheduler(10, 1, 1)
	defer scheduler.Stop()

	// Verify jobQueue is initialized
	assert.NotNil(t, scheduler.jobQueue, "Expected job queue to be non-nil")

	// Verify taskChan is initialized and has the correct buffer size
	assert.NotNil(t, scheduler.taskChan, "Expected task channel to be non-nil")
	taskChanBuffer := getChannelBufferSize(scheduler.taskChan)
	assert.Equal(t, 1, taskChanBuffer, "Expected task channel to have buffer size 1")

	// Verify resultChan is initialized and has the correct buffer size
	assert.NotNil(t, scheduler.resultChan, "Expected result channel to be non-nil")
	resultChanBuffer := getChannelBufferSize(scheduler.resultChan)
	assert.Equal(t, 1, resultChanBuffer, "Expected result channel to have buffer size 1")
}

func TestSchedulerStop(t *testing.T) {
	// NewSceduler starts the scheduler
	scheduler := NewScheduler(10, 2, 1)

	// Immediately stop the scheduler
	scheduler.Stop()

	// Attempt to add a task after stopping
	testChan := make(chan bool)
	testTask := MockTask{ID: "a-task", cadence: 50 * time.Millisecond, executeFunc: func() {
		testChan <- true
	}}
	scheduler.AddTask(testTask, testTask.cadence)

	// Since the scheduler is stopped, the task should not have been added to the job queue
	if scheduler.jobQueue.Len() != 0 {
		t.Fatalf("Expected job queue length to be 0, got %d", scheduler.jobQueue.Len())
	}

	// Wait some time to see if the task executes
	select {
	case <-testChan:
		t.Fatal("Did not expect any tasks to be executed after scheduler is stopped")
	case <-time.After(100 * time.Millisecond):
		// No tasks executed, as expected
	}
}

func TestAddFunc(t *testing.T) {
	scheduler := NewScheduler(10, 2, 1)
	defer scheduler.Stop()

	function := func() Result {
		return Result{Success: true}
	}
	cadence := 100 * time.Millisecond
	jobID := scheduler.AddFunc(function, cadence)

	assert.Equal(t, 1, scheduler.jobQueue.Len(), "Expected job queue length to be 1, got %d", scheduler.jobQueue.Len())

	job := scheduler.jobQueue[0]
	assert.Equal(t, 1, len(job.Tasks), "Expected job to have 1 task, got %d", len(job.Tasks))
	assert.Equal(t, jobID, job.ID, "Expected job ID to be %s, got %s", jobID, job.ID)
}

func TestAddTask(t *testing.T) {
	scheduler := NewScheduler(10, 2, 1)
	defer scheduler.Stop()

	testTask := MockTask{ID: "test-task", cadence: 100 * time.Millisecond}
	jobID := scheduler.AddTask(testTask, testTask.cadence)

	assert.Equal(t, 1, scheduler.jobQueue.Len(), "Expected job queue length to be 1, got %d", scheduler.jobQueue.Len())

	job := scheduler.jobQueue[0]
	assert.Equal(t, 1, len(job.Tasks), "Expected job to have 1 task, got %d", len(job.Tasks))
	assert.Equal(t, testTask, job.Tasks[0], "Expected the task in the job to be the test task")
	assert.Equal(t, jobID, job.ID, "Expected job ID to be %s, got %s", jobID, job.ID)
}

func TestAddJob(t *testing.T) {
	scheduler := NewScheduler(10, 2, 1)
	defer scheduler.Stop()

	mockTasks := []MockTask{
		{ID: "task1", cadence: 100 * time.Millisecond},
		{ID: "task2", cadence: 100 * time.Millisecond},
	}
	// Explicitly convert []MockTask to []Task to satisfy the AddJob method signature, since slices are not covariant in Go
	tasks := make([]Task, len(mockTasks))
	for i, task := range mockTasks {
		tasks[i] = task
	}
	jobID := scheduler.AddJob(tasks, 100*time.Millisecond)

	// Assert that the job was added
	assert.Equal(t, 1, scheduler.jobQueue.Len(), "Expected job queue length to be 1, got %d", scheduler.jobQueue.Len())
	job := scheduler.jobQueue[0]
	assert.Equal(t, 2, len(job.Tasks), "Expected job to have 2 tasks, got %d", len(job.Tasks))
	assert.Equal(t, jobID, job.ID, "Expected job ID to be %s, got %s", jobID, job.ID)
}

func TestRemoveJob(t *testing.T) {
	scheduler := NewScheduler(10, 2, 1)
	defer scheduler.Stop()

	mockTasks := []MockTask{
		{ID: "task1", cadence: 100 * time.Millisecond},
		{ID: "task2", cadence: 100 * time.Millisecond},
	}
	// Explicitly convert []MockTask to []Task to satisfy the AddJob method signature, since slices are not covariant in Go
	tasks := make([]Task, len(mockTasks))
	for i, task := range mockTasks {
		tasks[i] = task
	}
	jobID := scheduler.AddJob(tasks, 100*time.Millisecond)

	// Assert that the job was added
	assert.Equal(t, 1, scheduler.jobQueue.Len(), "Expected job queue length to be 1, got %d", scheduler.jobQueue.Len())
	job := scheduler.jobQueue[0]
	assert.Equal(t, jobID, job.ID, "Expected job ID to be %s, got %s", jobID, job.ID)
	assert.Equal(t, 2, len(job.Tasks), "Expected job to have 2 tasks, got %d", len(job.Tasks))

	// Remove the job
	scheduler.RemoveJob(jobID)

	// Assert that the job was removed
	assert.Equal(t, 0, scheduler.jobQueue.Len(), "Expected job queue length to be 0, got %d", scheduler.jobQueue.Len())
}

func TestTaskExecution(t *testing.T) {
	scheduler := NewScheduler(10, 1, 1)
	defer scheduler.Stop()

	var wg sync.WaitGroup
	wg.Add(1)

	// Use a channel to receive execution times
	executionTimes := make(chan time.Time, 1)

	// Create a test task
	testTask := &MockTask{
		ID:      "test-execution-task",
		cadence: 100 * time.Millisecond,
		executeFunc: func() {
			log.Debug().Msg("Executing TestTaskExecution task")
			executionTimes <- time.Now()
			wg.Done()
		},
	}

	scheduler.AddTask(testTask, testTask.cadence)

	select {
	case execTime := <-executionTimes:
		elapsed := time.Since(execTime)
		assert.Greater(t, 150*time.Millisecond, elapsed, "Task executed after %v, expected around 100ms", elapsed)
	case <-time.After(200 * time.Millisecond):
		t.Fatal("Task did not execute in expected time")
	}
}

func TestTaskRescheduling(t *testing.T) {
	// Make room in buffered channel for multiple results, since we're not consuming them in this test
	// and the results channel otherwise blocks the workers from executing tasks
	scheduler := NewScheduler(10, 1, 4)
	defer scheduler.Stop()

	var executionTimes []time.Time
	var mu sync.Mutex

	// Create a test task that records execution times
	mockTask := &MockTask{
		ID:      "test-rescheduling-task",
		cadence: 100 * time.Millisecond,
		executeFunc: func() {
			mu.Lock()
			executionTimes = append(executionTimes, time.Now())
			mu.Unlock()
		},
	}

	scheduler.AddTask(mockTask, mockTask.cadence)

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

// TODO: clean test up, removing some logs
func TestAddTaskDuringExecution(t *testing.T) {
	scheduler := NewScheduler(10, 1, 1)
	defer scheduler.Stop()

	// Consume resultChan to prevent workers from blocking
	go func() {
		for range scheduler.Results() {
			// Do nothing
		}
	}()

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
		executeFunc: func() {
			select {
			case <-ctx.Done():
				return
			default:
				task1Executed <- struct{}{}
				mu.Lock()
				trackTasks["task1"] = true
				mu.Unlock()
			}
		},
	}
	testTask2 := &MockTask{
		ID:      "test-add-mid-execution-task-2",
		cadence: 10 * time.Millisecond,
		executeFunc: func() {
			select {
			case <-ctx.Done():
				return
			default:
				mu.Lock()
				trackTasks["task2"] = true
				mu.Unlock()
			}
		},
	}

	// Schedule first task
	scheduler.AddTask(testTask1, testTask1.cadence)
	start := time.Now()

	select {
	case <-task1Executed:
		// Task executed as expected
	case <-time.After(20 * time.Millisecond):
		t.Fatalf("Task 1 did not execute within the expected time frame (40ms), %v", time.Since(start))
	}

	// Consume task1Executed to prevent it from blocking
	go func() {
		for range task1Executed {
			// Do nothing
		}
	}()

	// Schedule second task after we've had at least one execution of the first task
	scheduler.AddTask(testTask2, testTask2.cadence)

	// Sleep enough time to make sure both tasks have executed at least once
	time.Sleep(30 * time.Millisecond)

	cancel()
	scheduler.Stop()

	// Check that both tasks have executed
	mu.Lock()
	for msg, executed := range trackTasks {
		if !executed {
			t.Fatalf("Task '%s' did not execute, %v", msg, time.Since(start))
		}
	}
	mu.Unlock()
}

func TestConcurrentAddTask(t *testing.T) {
	// Deactivate debug logs for this test
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: "15:04:05.999"}).Level(zerolog.InfoLevel)

	scheduler := NewScheduler(10, 1, 1)
	defer scheduler.Stop()

	var wg sync.WaitGroup
	numGoroutines := 20
	numTasksPerGoroutine := 100

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numTasksPerGoroutine; j++ {
				taskID := fmt.Sprintf("task-%d-%d", id, j)
				task := MockTask{ID: taskID, cadence: 100 * time.Millisecond}
				scheduler.AddTask(task, task.cadence)
			}
		}(i)
	}

	wg.Wait()

	// Verify that all tasks are scheduled
	expectedTasks := numGoroutines * numTasksPerGoroutine
	assert.Equal(t, expectedTasks, scheduler.jobQueue.Len(), "Expected job queue length to be %d, got %d", expectedTasks, scheduler.jobQueue.Len())
}

func TestZeroCadenceTask(t *testing.T) {
	scheduler := NewScheduler(10, 1, 1)
	defer scheduler.Stop()

	testChan := make(chan bool)
	testTask := MockTask{ID: "zero-cadence-task", cadence: 0, executeFunc: func() {
		testChan <- true
	}}
	scheduler.AddTask(testTask, testTask.cadence)

	// Expect the task to not execute
	select {
	case <-testChan:
		// Task executed, which is unexpected
		t.Fatal("Task with zero cadence should not execute")
	case <-time.After(50 * time.Millisecond):
		// After 50ms, the task would have executed if it was scheduled
		log.Debug().Msg("Task with zero cadence never executed")
	}
}
