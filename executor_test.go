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

type executorTestSuite struct {
	newExec func() executor
}

func (s *executorTestSuite) TestExecutorSchedule(t *testing.T) {
	exec := s.newExec()
	defer exec.Stop()
	exec.Start()

	job := getMockedJob(2, "test-job", 100*time.Millisecond, 100*time.Millisecond)

	t.Run("valid scheduling", func(t *testing.T) {
		assert.NoError(t, exec.Schedule(job))
		// Assert that the job was added
		assert.Equal(t, 1, exec.Metrics().ManagedJobs, "Expected job queue length to be 1, got %d",
			exec.Metrics().ManagedJobs)
		scheduledJob, err := exec.Job(job.ID)
		assert.NoError(t, err)
		assert.Equal(t, len(job.Tasks), len(scheduledJob.Tasks),
			len(job.Tasks), "Expected job to have 2 tasks, got %d")
		assert.Equal(t, job.ID, scheduledJob.ID, "Expected job ID to be %s, got %s",
			scheduledJob.ID, job.ID)
	})

	t.Run("duplicate job", func(t *testing.T) {
		assert.Error(t, exec.Schedule(job), "Expected error for duplicate job ID")
	})
}

func (s *executorTestSuite) TestExecutorRemove(t *testing.T) {
	exec := s.newExec()
	defer exec.Stop()
	exec.Start()

	job := getMockedJob(2, "someJob", 100*time.Millisecond, 100*time.Millisecond)
	err := exec.Schedule(job)
	assert.NoError(t, err)

	// Assert that the job was added
	assert.Equal(t, 1, exec.Metrics().ManagedJobs, "Expected job queue length to be 1, got %d",
		exec.Metrics().ManagedJobs)
	qJob, err := exec.Job(job.ID)
	assert.NoError(t, err)
	assert.Equal(t, job.ID, qJob.ID, "Expected job ID to be %s, got %s", job.ID, qJob.ID)
	assert.Equal(t, 2, len(qJob.Tasks), "Expected job to have 2 tasks, got %d", len(qJob.Tasks))

	// Remove the job
	err = exec.Remove(job.ID)
	assert.NoError(t, err)

	// Assert that the job was removed
	assert.Equal(t, 0, exec.Metrics().ManagedJobs, "Expected job queue length to be 0, got %d",
		exec.Metrics().ManagedJobs)

	// Try removing the job once more
	err = exec.Remove(job.ID)
	assert.Error(t, err, "Expected removal of non-existent job to produce an error")
}

func (s *executorTestSuite) TestExecutorReplace(t *testing.T) {
	exec := s.newExec()
	defer exec.Stop()
	exec.Start()

	// Add a job
	firstJob := getMockedJob(2, "aJobID", 100*time.Millisecond, 100*time.Millisecond)
	err := exec.Schedule(firstJob)
	assert.NoError(t, err)
	// Assert job added
	assert.Equal(t, 1, exec.Metrics().ManagedJobs, "Expected job queue length to be 1, got %d",
		exec.Metrics().ManagedJobs)
	qJob, err := exec.Job(firstJob.ID)
	assert.NoError(t, err)
	assert.Equal(t, firstJob.ID, qJob.ID, "Expected job ID to be %s, got %s", firstJob.ID, qJob.ID)

	// Replace the first job
	secondJob := getMockedJob(4, "aJobID", 50*time.Millisecond, 100*time.Millisecond)
	err = exec.Replace(secondJob)
	assert.NoError(t, err)
	// Assert that the job was replaced in the queue
	assert.Equal(t, 1, exec.Metrics().ManagedJobs,
		"Expected job queue length to be 1, got %d", exec.Metrics().ManagedJobs)
	qJob, err = exec.Job(secondJob.ID)
	assert.NoError(t, err)
	assert.Equal(t, secondJob.ID, qJob.ID,
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

func (s *executorTestSuite) TestExecutorConcurrentSchedule(t *testing.T) {
	exec := s.newExec()
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
				// Use a long cadence to avoid task execution before test ends
				job := getMockedJob(2, taskID, 2*time.Second, 2*time.Second)
				assert.NoError(t, exec.Schedule(job), "Error adding job concurrently")
			}
		}(id)
	}

	wg.Wait()

	// Verify that all tasks are scheduled
	expectedTasks := numGoroutines * numTasksPerGoroutine
	assert.Equal(t, expectedTasks, exec.Metrics().ManagedJobs,
		"Expected job queue length to be %d, got %d",
		expectedTasks, exec.Metrics().ManagedJobs)
}

// runExecutorTestSuite runs all tests in the suite.
func runExecutorTestSuite(t *testing.T, s *executorTestSuite) {
	t.Run("Schedule", s.TestExecutorSchedule)
	t.Run("Remove", s.TestExecutorRemove)
	t.Run("Replace", s.TestExecutorReplace)
	t.Run("ConcurrentSchedule", s.TestExecutorConcurrentSchedule)
}

func TestExecutor(t *testing.T) {
	t.Run("PoolExecutor", func(t *testing.T) {
		newExec := func() executor {
			return getPoolExecutor()
		}
		runExecutorTestSuite(t, &executorTestSuite{newExec: newExec})
	})

	t.Run("DistributedExecutor", func(t *testing.T) {
		newExec := func() executor {
			return getDistExecutor()
		}
		runExecutorTestSuite(t, &executorTestSuite{newExec: newExec})
	})

	/* t.Run("SingleExecutor", func(t *testing.T) {
		newExec := func() executor {
			return newSingleExecutor(
				context.Background(),
				zerolog.Nop(),
				make(chan error),
				&managerMetrics{},
			)
		}
		runSliceTestSuite(t, &executorTestSuite{newExec: newExec})
	}) */
}

// Benchmarks

func benchID(i int) string { return "bench-" + itoa(i) }

func benchmarkSchedule(b *testing.B, factory func() executor) {
	exec := factory()
	defer exec.Stop()
	exec.Start()

	const cadence = 10 * time.Second

	for i := 0; b.Loop(); i++ {
		job := getMockedJob(1, benchID(i), cadence, cadence)
		if err := exec.Schedule(job); err != nil {
			b.Fatalf("schedule failed: %v", err)
		}
	}
	b.StopTimer()
}

func BenchmarkExecutorSchedule(b *testing.B) {
	b.Run("Pool", func(b *testing.B) { benchmarkSchedule(b, getPoolExecutor) })
	b.Run("Distributed", func(b *testing.B) { benchmarkSchedule(b, getDistExecutor) })
}

func benchmarkExecute(b *testing.B, factory func() executor) {
	exec := factory()
	defer exec.Stop()
	defer func() { time.Sleep(50 * time.Millisecond) }()
	exec.Start()

	const cadence = 5 * time.Millisecond
	taskFn := func() error { return nil }

	job := Job{
		Tasks:    []Task{simpleTask{taskFn}, simpleTask{taskFn}},
		Cadence:  cadence,
		ID:       benchID(0),
		NextExec: time.Now().Add(cadence),
	}
	if err := exec.Schedule(job); err != nil {
		b.Fatalf("schedule failed: %v", err)
	}
	time.Sleep(cadence)

	b.ResetTimer()
	for i := 1; i <= b.N; i++ {
		j := Job{
			Tasks:    []Task{simpleTask{taskFn}},
			Cadence:  cadence,
			ID:       benchID(i),
			NextExec: time.Now().Add(cadence),
		}
		if err := exec.Schedule(j); err != nil {
			b.Fatalf("schedule failed: %v", err)
		}
	}
}

func BenchmarkExecutorExecute(b *testing.B) {
	b.Run("Pool", func(b *testing.B) { benchmarkExecute(b, getPoolExecutor) })
	b.Run("Distributed", func(b *testing.B) { benchmarkExecute(b, getDistExecutor) })
}

// Lightweight int->string for benchmark IDs
var digits = [...]byte{'0', '1', '2', '3', '4', '5', '6', '7', '8', '9'}

func itoa(x int) string {
	if x == 0 {
		return "0"
	}
	buf := make([]byte, 0, 20)
	for x > 0 {
		buf = append(buf, digits[x%10])
		x /= 10
	}
	for i, j := 0, len(buf)-1; i < j; i, j = i+1, j-1 {
		buf[i], buf[j] = buf[j], buf[i]
	}
	return string(buf)
}

// Helpers

func getPoolExecutor() executor {
	return newPoolExecutor(
		context.Background(),
		zerolog.Nop(),
		make(chan error, defaultBufferSize),
		&managerMetrics{},
		2,
		10,
		time.Minute,
	)
}

func getDistExecutor() executor {
	return newDistributedExecutor(
		context.Background(),
		zerolog.Nop(),
		make(chan error, defaultBufferSize),
		&managerMetrics{},
		1,
		true,
		0,
	)
}
