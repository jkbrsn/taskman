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

// ConcurrentExecution schedules many small jobs with short cadence and waits for executions.
func (s *executorTestSuite) TestExecutorConcurrentExecution(t *testing.T) {
	exec := s.newExec()
	defer exec.Stop()
	exec.Start()

	// Small job workload
	taskFn := func() error { return nil }
	cadence := 5 * time.Millisecond

	numGoroutines := 10
	numJobsPerGoroutine := 100

	var wg sync.WaitGroup
	for id := range numGoroutines {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := range numJobsPerGoroutine {
				jobID := fmt.Sprintf("exec-%d-%d", id, j)
				job := Job{
					Tasks:    []Task{simpleTask{taskFn}},
					Cadence:  cadence,
					ID:       jobID,
					NextExec: time.Now().Add(cadence),
				}
				assert.NoError(t, exec.Schedule(job))
			}
		}(id)
	}
	wg.Wait()

	// Allow at least one tick for all jobs
	time.Sleep(cadence * 2)

	// Basic sanity: all jobs scheduled
	expectedJobs := numGoroutines * numJobsPerGoroutine
	assert.Equal(t, expectedJobs, exec.Metrics().ManagedJobs,
		"Expected %d jobs scheduled, got %d", expectedJobs, exec.Metrics().ManagedJobs)
}

// NextExec variants coverage for executors
func (s *executorTestSuite) TestExecutorNextExecVariants(t *testing.T) {
	exec := s.newExec()
	defer exec.Stop()
	exec.Start()

	t.Run("Zero NextExec sets to now+cadence", func(t *testing.T) {
		cadence := 40 * time.Millisecond
		job := Job{
			ID:       "next-zero",
			Cadence:  cadence,
			NextExec: time.Time{},
			Tasks:    []Task{simpleTask{func() error { return nil }}},
		}
		start := time.Now()
		assert.NoError(t, exec.Schedule(job))
		qJob, err := exec.Job(job.ID)
		assert.NoError(t, err)
		delay := qJob.NextExec.Sub(start)
		assert.GreaterOrEqual(t, int64(delay), int64(cadence))
		assert.LessOrEqual(t, delay, cadence+50*time.Millisecond)
		assert.NoError(t, exec.Remove(job.ID))
	})

	t.Run("Past within cadence executes immediately", func(t *testing.T) {
		cadence := 10 * time.Millisecond
		done := make(chan struct{}, 1)
		job := Job{
			ID:       "next-past-within",
			Cadence:  cadence,
			NextExec: time.Now().Add(-cadence / 2),
			Tasks:    []Task{simpleTask{func() error { done <- struct{}{}; return nil }}},
		}
		assert.NoError(t, exec.Schedule(job))
		select {
		case <-done:
			// ok
		case <-time.After(cadence):
			t.Fatal("Job did not execute immediately")
		}
		assert.NoError(t, exec.Remove(job.ID))
	})

	t.Run("Past more than cadence rejected", func(t *testing.T) {
		cadence := 10 * time.Millisecond
		job := Job{
			ID:       "next-too-old",
			Cadence:  cadence,
			NextExec: time.Now().Add(-2 * cadence),
			Tasks:    []Task{simpleTask{func() error { return nil }}},
		}
		err := exec.Schedule(job)
		assert.Error(t, err)
	})

	t.Run("Delayed future execution", func(t *testing.T) {
		// Use a channel to receive execution times
		executionTimes := make(chan time.Time, 1)
		start := time.Now()
		execDelay := 10 * time.Millisecond
		job := Job{
			ID:       "test-instant-execution-4",
			Cadence:  5 * time.Millisecond,
			NextExec: time.Now().Add(execDelay),
			Tasks: []Task{MockTask{ID: "task1", executeFunc: func() error {
				executionTimes <- time.Now()
				return nil
			}}},
		}
		err := exec.Schedule(job)
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

// runExecutorTestSuite runs all tests in the suite.
func runExecutorTestSuite(t *testing.T, s *executorTestSuite) {
	t.Run("Schedule", s.TestExecutorSchedule)
	t.Run("Remove", s.TestExecutorRemove)
	t.Run("Replace", s.TestExecutorReplace)
	t.Run("ConcurrentSchedule", s.TestExecutorConcurrentSchedule)
	t.Run("ConcurrentExecution", s.TestExecutorConcurrentExecution)
	t.Run("NextExecVariants", s.TestExecutorNextExecVariants)
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

	t.Run("OnDemandExecutor", func(t *testing.T) {
		newExec := func() executor {
			return getOnDemandExecutor()
		}
		runExecutorTestSuite(t, &executorTestSuite{newExec: newExec})
	})
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
	b.Run("OnDemand", func(b *testing.B) { benchmarkSchedule(b, getOnDemandExecutor) })
}

func benchmarkExecute(b *testing.B, factory func() executor) {
	exec := factory()
	defer exec.Stop()
	exec.Start()

	const (
		cadence   = 5 * time.Millisecond
		jobCount  = 500
		safeDelay = 50 * time.Millisecond
	)
	taskFn := func() error { return nil }
	safeStart := time.Now().Add(safeDelay)

	// Pre-schedule a fixed number of jobs so execution is bounded.
	for i := range jobCount {
		j := Job{
			Tasks:    []Task{simpleTask{taskFn}},
			Cadence:  cadence,
			ID:       benchID(i),
			NextExec: safeStart,
		}
		if err := exec.Schedule(j); err != nil {
			b.Fatalf("schedule failed: %v", err)
		}
	}

	// Warm up until first executions
	time.Sleep(3 * cadence)

	for b.Loop() {
		// Let the system run for one cadence per iteration to measure steady-state cost
		time.Sleep(cadence)
	}
}

func BenchmarkExecutorExecute(b *testing.B) {
	b.Run("Pool", func(b *testing.B) { benchmarkExecute(b, getPoolExecutor) })
	b.Run("Distributed", func(b *testing.B) { benchmarkExecute(b, getDistExecutor) })
	b.Run("OnDemand", func(b *testing.B) { benchmarkExecute(b, getOnDemandExecutor) })
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
		x /= 10 // nolint: revive
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
		&executorMetrics{},
		2,
		10,
		time.Minute,
		defaultPoolScaleCfg(),
	)
}

func getDistExecutor() executor {
	return newDistributedExecutor(
		context.Background(),
		zerolog.Nop(),
		make(chan error, defaultBufferSize),
		&executorMetrics{},
		defaultBufferSize,
		1,
		true,
		0,
	)
}

func getOnDemandExecutor() executor {
	return newOnDemandExecutor(
		context.Background(),
		zerolog.Nop(),
		make(chan error, defaultBufferSize),
		&executorMetrics{},
		defaultBufferSize,
		1,
		true,
		0,
	)
}
