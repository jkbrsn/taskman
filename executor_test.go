package taskman

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
)

// TODO: metrics test for executor interface?

func TestExecutorSchedule(t *testing.T) {
	exec := newPoolExecutor(
		context.Background(),
		testLogger,
		make(chan error),
		&managerMetrics{},
		2,
		10,
		1*time.Minute,
	)
	defer exec.Stop()
	exec.Start()

	job := getMockedJob(2, "test-job", 100*time.Millisecond, 100*time.Millisecond)

	t.Run("valid scheduling", func(t *testing.T) {
		assert.NoError(t, exec.Schedule(job))
		// Assert that the job was added
		assert.Equal(t, 1, exec.jobsInQueue(), "Expected job queue length to be 1, got %d",
			exec.jobsInQueue())
		scheduledJob := exec.jobQueue[0]
		assert.Equal(t, len(job.Tasks), len(scheduledJob.Tasks),
			len(job.Tasks), "Expected job to have 2 tasks, got %d")
		assert.Equal(t, job.ID, scheduledJob.ID, "Expected job ID to be %s, got %s",
			scheduledJob.ID, job.ID)
	})

	t.Run("duplicate job", func(t *testing.T) {
		assert.Error(t, exec.Schedule(job), "Expected error for duplicate job ID")
	})
}

func TestExecutorRemove(t *testing.T) {
	exec := newPoolExecutor(
		context.Background(),
		zerolog.Nop(),
		make(chan error),
		&managerMetrics{},
		2,
		10,
		1*time.Minute,
	)
	defer exec.Stop()
	exec.Start()

	job := getMockedJob(2, "someJob", 100*time.Millisecond, 100*time.Millisecond)
	err := exec.Schedule(job)
	assert.Nil(t, err, "Error adding job")

	// Assert that the job was added
	assert.Equal(t, 1, exec.jobsInQueue(), "Expected job queue length to be 1, got %d",
		exec.jobsInQueue())
	qJob := exec.jobQueue[0]
	assert.Equal(t, job.ID, qJob.ID, "Expected job ID to be %s, got %s", job.ID, qJob.ID)
	assert.Equal(t, 2, len(qJob.Tasks), "Expected job to have 2 tasks, got %d", len(qJob.Tasks))

	// Remove the job
	err = exec.Remove(job.ID)
	assert.Nil(t, err, "Error removing job")

	// Assert that the job was removed
	assert.Equal(t, 0, exec.jobsInQueue(), "Expected job queue length to be 0, got %d",
		exec.jobsInQueue())

	// Try removing the job once more
	err = exec.Remove(job.ID)
	assert.Error(t, err, "Expected removal of non-existent job to produce an error")
}

func TestExecutorReplace(t *testing.T) {
	exec := newPoolExecutor(
		context.Background(),
		zerolog.Nop(),
		make(chan error),
		&managerMetrics{},
		4,
		4,
		1*time.Minute,
	)
	defer exec.Stop()
	exec.Start()

	// Add a job
	firstJob := getMockedJob(2, "aJobID", 100*time.Millisecond, 100*time.Millisecond)
	err := exec.Schedule(firstJob)
	assert.Nil(t, err, "Error adding job")
	// Assert job added
	assert.Equal(t, 1, exec.jobsInQueue(), "Expected job queue length to be 1, got %d",
		exec.jobsInQueue())
	qJob := exec.jobQueue[0]
	assert.Equal(t, firstJob.ID, qJob.ID, "Expected ID to be '%s', got '%s'", firstJob.ID, qJob.ID)

	// Replace the first job
	secondJob := getMockedJob(4, "aJobID", 50*time.Millisecond, 100*time.Millisecond)
	err = exec.Replace(secondJob)
	assert.Nil(t, err, "Error replacing job")
	// Assert that the job was replaced in the queue
	assert.Equal(t, 1, exec.jobsInQueue(),
		"Expected job queue length to be 1, got %d", exec.jobsInQueue())
	qJob = exec.jobQueue[0]
	// The queue job should retain the index and NextExec time of the first job
	assert.Equal(t, firstJob.index, qJob.index,
		"Expected index to be '%s', got '%s'", secondJob.index, qJob.index)
	assert.Equal(t, firstJob.NextExec, qJob.NextExec,
		"Expected ID to be '%s', got '%s'", secondJob.NextExec, qJob.NextExec)
	// The queue job should have the ID, cadence and tasks of the new (second) job
	assert.Equal(t, secondJob.ID, qJob.ID,
		"Expected ID to be '%s', got '%s'", secondJob.ID, qJob.ID)
	assert.Equal(t, secondJob.Cadence, qJob.Cadence,
		"Expected cadence to be '%s', got '%s'", secondJob.Cadence, qJob.Cadence)
	assert.Equal(t, len(secondJob.Tasks), len(qJob.Tasks),
		"Expected job to have %d tasks, got %d", len(secondJob.Tasks), len(qJob.Tasks))

	// Try to replace a non-existing job
	thirdJob := getMockedJob(2, "anotherJobID", 10*time.Millisecond, 100*time.Millisecond)
	err = exec.Replace(thirdJob)
	assert.Error(t, err, "Expected replace attempt of non-existent job to produce an error")
}

func TestExecutorConcurrentSchedule(t *testing.T) {
	exec := newPoolExecutor(
		context.Background(),
		zerolog.Nop(),
		make(chan error),
		&managerMetrics{},
		1,
		10,
		1*time.Minute,
	)
	defer exec.Stop()
	exec.Start()

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
				job := getMockedJob(2, taskID, 2*time.Second, 2*time.Second)
				assert.NoError(t, exec.Schedule(job), "Error adding job concurrently")
			}
		}(id)
	}

	wg.Wait()

	// Verify that all tasks are scheduled
	expectedTasks := numGoroutines * numTasksPerGoroutine
	assert.Equal(t, expectedTasks, exec.jobsInQueue(),
		"Expected job queue length to be %d, got %d",
		expectedTasks, exec.jobsInQueue())
}
