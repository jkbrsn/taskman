package taskman

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
)

// TODO: metrics test for poolExecutor?

func TestPoolExecutorReplace(t *testing.T) {
	// Setup executor
	exec := newPoolExecutor(
		context.Background(),
		zerolog.Nop(),
		make(chan error),
		&managerMetrics{},
		4,
		1,
		1*time.Minute,
	)
	defer exec.Stop()
	exec.Start()

	// Schedule initial job
	orig := Job{
		ID:       "job-replace",
		Cadence:  50 * time.Millisecond,
		NextExec: time.Now().Add(30 * time.Millisecond),
		Tasks:    []Task{MockTask{ID: "t1"}},
	}
	assert.NoError(t, exec.Schedule(orig))

	// Snapshot current head NextExec and index before replace
	time.Sleep(2 * time.Millisecond)
	exec.mu.RLock()
	idx, err := exec.jobQueue.JobInQueue("job-replace")
	assert.NoError(t, err)
	oldNext := exec.jobQueue[idx].NextExec
	exec.mu.RUnlock()

	// Replace with wider job and different cadence; NextExec should be preserved
	repl := Job{
		ID:      "job-replace",
		Cadence: 10 * time.Millisecond,
		Tasks:   []Task{MockTask{ID: "t1"}, MockTask{ID: "t2"}},
	}
	assert.NoError(t, exec.Replace(repl))

	// Verify fields and heap invariants preserved
	exec.mu.RLock()
	idx2, err2 := exec.jobQueue.JobInQueue("job-replace")
	assert.NoError(t, err2)
	got := exec.jobQueue[idx2]
	assert.Equal(t, oldNext, got.NextExec, "NextExec must be preserved on Replace")
	// Ensure the job's stored index matches its position
	assert.Equal(t, idx2, got.index, "heap index must match position after Replace")
	exec.mu.RUnlock()
}

func TestPoolExecutorWorkerPoolScaling(t *testing.T) {
	// Start a pool executor with 1 worker
	exec := newPoolExecutor(
		context.Background(),
		zerolog.Nop(),
		make(chan error),
		&managerMetrics{},
		4,
		1,
		1*time.Minute,
	)
	defer exec.Stop()
	exec.Start()

	// The first two test cases sets cadences and task execution duration to values
	// producing a predetermined number of needed workers. The third test case uses
	// a larger number of tasks to force scaling up. The fourth test case uses
	// removes jobs to force scaling down.
	t.Run("ScaleUpBasedOnJobWidth", func(t *testing.T) {
		job := Job{
			ID:       "test-job-width-scaling",
			Cadence:  5 * time.Millisecond,
			NextExec: time.Now().Add(20 * time.Millisecond),
			Tasks: []Task{MockTask{ID: "task1", executeFunc: func() error {
				testLogger.Debug().Msg("Executing task1")
				time.Sleep(20 * time.Millisecond) // Simulate 20 ms execution time
				return nil
			}}, MockTask{ID: "task2", executeFunc: func() error {
				testLogger.Debug().Msg("Executing task2")
				time.Sleep(20 * time.Millisecond) // Simulate 20 ms execution time
				return nil
			}}},
		}
		err := exec.Schedule(job)
		assert.Nil(t, err, "Expected no error scheduling job")
		time.Sleep(5 * time.Millisecond) // Allow time for job to be scheduled +
		// worker pool to scale

		assert.Equal(
			t,
			exec.workerPool.targetWorkerCount(),
			int32(4),
			"Expected target worker count to be 2 x the job task count",
		)
	})

	t.Run("ScaleUpBasedOnConcurrencyNeeds", func(t *testing.T) {
		initialTargetWorkerCount := exec.workerPool.targetWorkerCount()
		job := Job{
			ID:       "test-concurrency-need-scaling",
			Cadence:  5 * time.Millisecond, // Set a low cadence to force scaling up
			NextExec: time.Now().Add(10 * time.Millisecond),
			Tasks: []Task{MockTask{ID: "task3", executeFunc: func() error {
				testLogger.Debug().Msg("Executing task3")
				time.Sleep(20 * time.Millisecond) // Simulate 20 ms execution time
				return nil
			}}},
		}
		err := exec.Schedule(job)
		assert.Nil(t, err, "Expected no error scheduling job")

		// Allow time for job to be scheduled + executed, then scale again
		time.Sleep(45 * time.Millisecond)
		exec.scaleWorkerPool(0)
		time.Sleep(5 * time.Millisecond) // Allow time for worker pool to scale

		// Check greater than rather than an exact number, as computation time may vary
		assert.Greater(
			t,
			exec.workerPool.targetWorkerCount(),
			initialTargetWorkerCount,
			"Expected target worker count to be greater than initial count",
		)
	})

	t.Run("ScaleUpBasedOnImmediateNeed", func(t *testing.T) {
		runningWorkers := exec.workerPool.runningWorkers()
		availableWorkers := exec.workerPool.availableWorkers()

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
		err := exec.Schedule(job)
		assert.Nil(t, err, "Expected no error scheduling job")

		// Allow time for job to be scheduled and scaling to take place
		time.Sleep(5 * time.Millisecond)

		// Expected worker count is 1.5 x (the current worker count + extra workers needed)
		// Note: we'll assert within a delta here, since the state of the worker pool variables may
		//       have changed since the time the pool was scaled.
		expectedWorkerCount := math.Ceil(
			(float64(runningWorkers) + (4.0 - float64(availableWorkers))) * 1.5,
		)
		assert.InDelta(
			t,
			expectedWorkerCount,
			exec.workerPool.targetWorkerCount(),
			2.0,
			"Expected target worker count to be max 2.0 away from %d",
			expectedWorkerCount,
		)
	})

	t.Run("ScaleDown", func(t *testing.T) {
		exec := newPoolExecutor(
			context.Background(),
			zerolog.Nop(),
			make(chan error),
			&managerMetrics{},
			1,
			1,
			1*time.Minute,
		)
		defer exec.Stop()
		exec.Start()

		job := Job{
			ID:       "test-job-width-scaling",
			Cadence:  5 * time.Millisecond,
			NextExec: time.Now().Add(20 * time.Millisecond),
			Tasks: []Task{MockTask{ID: "task1", executeFunc: func() error {
				testLogger.Debug().Msg("Executing task1")
				time.Sleep(20 * time.Millisecond) // Simulate 20 ms execution time
				return nil
			}}, MockTask{ID: "task2", executeFunc: func() error {
				testLogger.Debug().Msg("Executing task2")
				time.Sleep(20 * time.Millisecond) // Simulate 20 ms execution time
				return nil
			}}},
		}
		err := exec.Schedule(job)
		assert.Nil(t, err, "Expected no error scheduling job")
		time.Sleep(5 * time.Millisecond) // Allow time for job to be scheduled +
		// worker pool to scale

		initialRunningWorkers := exec.workerPool.runningWorkers()
		initialTargetWorkerCount := exec.workerPool.targetWorkerCount()

		// Remove the job
		err = exec.Remove("test-job-width-scaling")
		assert.Nil(t, err, "Expected no error removing job")
		time.Sleep(5 * time.Millisecond) // Allow time for job to be removed

		// Check that the worker pool has scaled down
		assert.Less(
			t,
			exec.workerPool.runningWorkers(),
			initialRunningWorkers,
			"Expected running worker count to be less than initial count",
		)
		assert.Less(
			t,
			exec.workerPool.targetWorkerCount(),
			initialTargetWorkerCount,
			"Expected target worker count to be less than initial count",
		)
	})

	t.Run("ScaleUpRespectsMaxWorkerCount", func(t *testing.T) {
		exec := newPoolExecutor(
			context.Background(),
			zerolog.Nop(),
			make(chan error),
			&managerMetrics{},
			1,
			1,
			1*time.Minute,
		)
		defer exec.Stop()
		exec.Start()

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
		err := exec.Schedule(largeJob)
		assert.NoError(t, err, "Expected no error scheduling job")

		// Allow time for job to be scheduled and scaling to take place
		time.Sleep(200 * time.Millisecond)

		// Verify that the worker count is capped at maxWorkerCount
		assert.Equal(t, int32(maxWorkerCount), exec.workerPool.targetWorkerCount(),
			"Expected target worker count to be capped at maxWorkerCount")
		assert.Equal(t, int32(maxWorkerCount), exec.workerPool.runningWorkers(),
			"Expected running worker count to be capped at maxWorkerCount")

		// Try to scale up further with another large job
		largeJob2 := Job{
			ID:       "test-max-worker-scaling-2",
			Cadence:  10 * time.Millisecond,
			NextExec: time.Now().Add(20 * time.Millisecond),
			Tasks:    make([]Task, maxWorkerCount+10),
		}
		err = exec.Schedule(largeJob2)
		assert.NoError(t, err, "Expected no error scheduling job")

		// Allow time for scaling to take place
		time.Sleep(25 * time.Millisecond)

		// Verify that the worker count is still capped at maxWorkerCount
		assert.Equal(t, int32(maxWorkerCount), exec.workerPool.targetWorkerCount(),
			"Expected target worker count to remain capped at maxWorkerCount")
		assert.Equal(t, int32(maxWorkerCount), exec.workerPool.runningWorkers(),
			"Expected running worker count to remain capped at maxWorkerCount")
	})

	t.Run("RemoveAllJobs", func(t *testing.T) {
		exec := newPoolExecutor(
			context.Background(),
			zerolog.Nop(),
			make(chan error),
			&managerMetrics{},
			1,
			1,
			1*time.Minute,
		)
		defer exec.Stop()
		exec.Start()

		job := Job{
			ID:       "test-job",
			Cadence:  10 * time.Millisecond,
			NextExec: time.Now().Add(20 * time.Millisecond),
			Tasks:    make([]Task, 10),
		}
		for i := range job.Tasks {
			job.Tasks[i] = MockTask{ID: fmt.Sprintf("task%d", i)}
		}
		err := exec.Schedule(job)
		assert.NoError(t, err)

		// Allow time for job to be scheduled and scaling to take place
		time.Sleep(5 * time.Millisecond)

		// Remove the job
		err = exec.Remove("test-job")
		assert.NoError(t, err, "Expected no error removing job")
		time.Sleep(150 * time.Millisecond) // Allow time for job removal

		// Check that the worker pool has scaled down
		assert.Equal(
			t,
			int32(exec.minWorkerCount),
			exec.workerPool.targetWorkerCount(),
			"Expected target worker count to be %d after removing all jobs",
			exec.minWorkerCount,
		)
	})
}

func TestPoolExecutorWorkerPoolPeriodicScaling(t *testing.T) {
	// Start a manager with 1 worker, and a scaling interval of 40ms. The scaling interval is set
	// to occur after the first job has executed at least once.
	exec := newPoolExecutor(
		context.Background(),
		zerolog.Nop(),
		make(chan error),
		&managerMetrics{},
		4,
		1,
		50*time.Millisecond,
	)
	defer exec.Stop()
	exec.Start()

	// Add a job with 4 x longer execution than cadence, resulting in at least 4 workers being
	// needed although only one additional will be added from scaling based on the job width
	job := Job{
		ID:       "test-periodic-scaling",
		Cadence:  5 * time.Millisecond,
		NextExec: time.Now().Add(20 * time.Millisecond),
		Tasks: []Task{MockTask{ID: "task1", executeFunc: func() error {
			testLogger.Debug().Msg("Executing task1")
			time.Sleep(20 * time.Millisecond) // Simulate 20 ms execution time
			return nil
		}}},
	}
	err := exec.Schedule(job)
	assert.Nil(t, err, "Expected no error scheduling job")
	time.Sleep(5 * time.Millisecond) // Allow time for job to be scheduled + worker pool to scale

	assert.Equal(
		t,
		exec.workerPool.targetWorkerCount(),
		int32(2),
		"Expected target worker count to be 2 x the job task count",
	)

	time.Sleep(50 * time.Millisecond) // Allow time for periodic scaling to occur

	assert.GreaterOrEqual(
		t,
		exec.workerPool.targetWorkerCount(),
		int32(4),
		"Expected target worker count to be greater or equal than 4",
	)
}
