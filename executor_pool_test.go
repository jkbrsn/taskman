package taskman

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TODO: metrics test for poolExecutor?

func TestPoolExecutorReplace(t *testing.T) {
	// Setup executor
	exec := newPoolExecutor(
		context.Background(),
		zerolog.Nop(),
		make(chan error),
		&executorMetrics{},
		4,
		1,
		1*time.Minute,
		defaultPoolScaleCfg(),
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

/*
func TestPoolExecutorWorkerPoolScaling(t *testing.T) {
	// Start a pool executor with 1 worker
	exec := newPoolExecutor(
		context.Background(),
		zerolog.Nop(),
		make(chan error),
		&executorMetrics{},
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
			&executorMetrics{},
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
			&executorMetrics{},
			1,
			1,
			1*time.Minute,
		)
		defer exec.Stop()
		exec.Start()

		// Create a job that would require more workers than defaultMaxWorkerCount
		largeJob := Job{
			ID:       "test-max-worker-scaling",
			Cadence:  10 * time.Millisecond,
			NextExec: time.Now().Add(20 * time.Millisecond),
			Tasks:    make([]Task, defaultMaxWorkerCount+10), // More tasks than defaultMaxWorkerCount
		}
		for i := range largeJob.Tasks {
			largeJob.Tasks[i] = MockTask{ID: fmt.Sprintf("task%d", i)}
		}
		err := exec.Schedule(largeJob)
		assert.NoError(t, err, "Expected no error scheduling job")

		// Allow time for job to be scheduled and scaling to take place
		time.Sleep(200 * time.Millisecond)

		// Verify that the worker count is capped at defaultMaxWorkerCount
		assert.Equal(t, int32(defaultMaxWorkerCount), exec.workerPool.targetWorkerCount(),
			"Expected target worker count to be capped at defaultMaxWorkerCount")
		assert.Equal(t, int32(defaultMaxWorkerCount), exec.workerPool.runningWorkers(),
			"Expected running worker count to be capped at defaultMaxWorkerCount")

		// Try to scale up further with another large job
		largeJob2 := Job{
			ID:       "test-max-worker-scaling-2",
			Cadence:  10 * time.Millisecond,
			NextExec: time.Now().Add(20 * time.Millisecond),
			Tasks:    make([]Task, defaultMaxWorkerCount+10),
		}
		err = exec.Schedule(largeJob2)
		assert.NoError(t, err, "Expected no error scheduling job")

		// Allow time for scaling to take place
		time.Sleep(25 * time.Millisecond)

		// Verify that the worker count is still capped at defaultMaxWorkerCount
		assert.Equal(t, int32(defaultMaxWorkerCount), exec.workerPool.targetWorkerCount(),
			"Expected target worker count to remain capped at defaultMaxWorkerCount")
		assert.Equal(t, int32(defaultMaxWorkerCount), exec.workerPool.runningWorkers(),
			"Expected running worker count to remain capped at defaultMaxWorkerCount")
	})

	t.Run("RemoveAllJobs", func(t *testing.T) {
		exec := newPoolExecutor(
			context.Background(),
			zerolog.Nop(),
			make(chan error),
			&executorMetrics{},
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
} */

func TestPoolExecutor_PoolScaler(t *testing.T) {
	// Helper to create a fresh executor with tiny intervals so tests run fast.
	newExec := func(minWorkers int) *poolExecutor {
		cfg := PoolScaleConfig{
			MinWorkers:          minWorkers,
			MaxWorkers:          defaultMaxWorkerCount,
			TargetUtilization:   0.70,
			DeadbandRatio:       0.10,
			CooldownUp:          0,                      // fast up
			CooldownDown:        200 * time.Millisecond, // slow/cautious down
			MaxStepUp:           0,                      // jump to target
			MaxStepDown:         1,                      // gentle down
			EWMAFastAlpha:       0.5,
			EWMASlowAlpha:       0.1,
			BurstHeadroomFactor: 1.25,
		}
		exec := newPoolExecutor(
			context.Background(),
			zerolog.Nop(),
			make(chan error, 128),
			&executorMetrics{},
			256,           // channelBufferSize
			minWorkers,    // minWorkerCount
			5*time.Second, // scaleInterval set high since we invoke scale() explicitly
			cfg,
		)
		exec.Start()

		// Reset scaler time anchors so first decisions aren’t suppressed by cooldown.
		exec.poolScaler.lastScaleUp = time.Time{}
		exec.poolScaler.lastScaleDown = time.Time{}

		return exec
	}

	t.Run("ScaleUpToDemand_FromThroughputAndExecTime", func(t *testing.T) {
		exec := newExec(1)
		defer exec.Stop()

		// One task every 5ms, each runs ~20ms ⇒ λ≈200/s, E[S]≈0.02
		// Needed ≈ ceil(λ*E[S]/0.7) ≈ ceil(4/0.7) ≈ 6
		job := Job{
			ID:       "burst-demand",
			Cadence:  2 * time.Millisecond,
			NextExec: time.Now().Add(5 * time.Millisecond),
			Tasks: []Task{
				MockTask{ID: "t", executeFunc: func() error {
					time.Sleep(30 * time.Millisecond)
					return nil
				}},
			},
		}
		require.NoError(t, exec.Schedule(job))

		// Let a couple of executions happen so metrics (TPS, avg exec) move.
		time.Sleep(120 * time.Millisecond)

		prev := exec.workerPool.workerCountTarget.Load()
		// Force a scale tick using current instantaneous metrics.
		exec.poolScaler.scale(time.Now(), 0)
		time.Sleep(20 * time.Millisecond)

		got := exec.workerPool.workerCountTarget.Load()
		require.Greater(t, got, prev, "expected target workers to increase (prev=%d, got=%d)", prev, got)
	})

	t.Run("ImmediatePressure_OverridesDemand", func(t *testing.T) {
		exec := newExec(1)
		defer exec.Stop()
		time.Sleep(10 * time.Millisecond)

		// Snapshot pre-schedule state
		prev := int(exec.workerPool.workerCountTarget.Load())
		avail0 := int(exec.workerPool.availableWorkers())
		running0 := int(exec.workerPool.runningWorkers())

		// Create a wide job with 6 tasks
		job := Job{
			ID:       "immediate-pressure",
			Cadence:  50 * time.Millisecond,
			NextExec: time.Now().Add(20 * time.Millisecond),
			Tasks: []Task{
				MockTask{ID: "a"}, MockTask{ID: "b"},
				MockTask{ID: "c"}, MockTask{ID: "d"},
				MockTask{ID: "e"}, MockTask{ID: "f"},
			},
		}
		// exec.Schedule calls scaler.scale() with "workersNeededNow = 6"
		require.NoError(t, exec.Schedule(job))
		time.Sleep(10 * time.Millisecond)

		got := int(exec.workerPool.workerCountTarget.Load())

		shortfall := max(6-avail0, 0)
		wantMin := running0 + shortfall

		if got <= prev || got < wantMin {
			t.Fatalf("expected target to increase and cover immediate need; prev=%d running0=%d avail0=%d wantMin=%d got=%d",
				prev, running0, avail0, wantMin, got)
		}
	})

	t.Run("Deadband_SuppressesSmallFluctuations", func(t *testing.T) {
		exec := newExec(4)
		defer exec.Stop()

		// Stabilize target roughly around 4–6 workers.
		job := Job{
			ID:       "steady",
			Cadence:  10 * time.Millisecond,
			NextExec: time.Now().Add(20 * time.Millisecond),
			Tasks: []Task{
				MockTask{ID: "t", executeFunc: func() error {
					time.Sleep(10 * time.Millisecond)
					return nil
				}},
			},
		}
		require.NoError(t, exec.Schedule(job))
		time.Sleep(200 * time.Millisecond)

		prev := int(exec.workerPool.workerCountTarget.Load())

		// Nudge metrics slightly by letting it run shortly; decision should be within deadband.
		exec.poolScaler.scale(time.Now(), 0)
		time.Sleep(20 * time.Millisecond)

		got := int(exec.workerPool.workerCountTarget.Load())
		// With 10% deadband, tiny shifts should not change target.
		require.Equal(t, prev, got, "expected no change within deadband; prev=%d got=%d", prev, got)
	})

	t.Run("CapAtMaxWorkerCount", func(t *testing.T) {
		exec := newExec(1)
		defer exec.Stop()

		// Create a job that implies more workers than defaultMaxWorkerCount.
		wide := Job{
			ID:       "cap-max",
			Cadence:  10 * time.Millisecond,
			NextExec: time.Now().Add(20 * time.Millisecond),
			Tasks:    make([]Task, defaultMaxWorkerCount+16),
		}
		for i := range wide.Tasks {
			wide.Tasks[i] = MockTask{ID: fmt.Sprintf("t%d", i)}
		}
		// exec.Schedule calls scaler.scale(), then we allow for startup
		require.NoError(t, exec.Schedule(wide))
		time.Sleep(100 * time.Millisecond)

		// Force a scale tick when wide job is running
		exec.poolScaler.scale(time.Now(), len(wide.Tasks))
		time.Sleep(20 * time.Millisecond)

		got := exec.workerPool.workerCountTarget.Load()
		assert.Equal(t, int32(defaultMaxWorkerCount), got,
			"expected cap at %d, got %d", defaultMaxWorkerCount, got)
		got = exec.workerPool.runningWorkers()
		// Allow some leeway considering the async nature of the worker pool
		require.InDelta(t, int32(defaultMaxWorkerCount), got, 2.0,
			"running workers must not exceed cap; got %d", got)
	})

	t.Run("ScaleDown_AfterRemovingJobs", func(t *testing.T) {
		exec := newExec(1)
		defer exec.Stop()

		job := Job{
			ID:       "remove-me",
			Cadence:  10 * time.Millisecond,
			NextExec: time.Now().Add(20 * time.Millisecond),
			Tasks: []Task{MockTask{ID: "t", executeFunc: func() error {
				time.Sleep(10 * time.Millisecond)
				return nil
			}}},
		}
		if err := exec.Schedule(job); err != nil {
			t.Fatalf("schedule: %v", err)
		}
		// Warm up a bit so it has reason to scale up.
		time.Sleep(200 * time.Millisecond)
		exec.poolScaler.scale(time.Now(), 0)
		time.Sleep(30 * time.Millisecond)

		prevTarget := exec.workerPool.workerCountTarget.Load()

		// Remove the job and wait past downscale cooldown.
		require.NoError(t, exec.Remove("remove-me"))

		time.Sleep(exec.poolScaler.cfg.CooldownDown + 150*time.Millisecond)
		exec.poolScaler.scale(time.Now(), 0)
		time.Sleep(30 * time.Millisecond)

		got := exec.workerPool.workerCountTarget.Load()
		if !(got < prevTarget && got >= int32(exec.minWorkerCount)) {
			t.Fatalf("expected downscale towards min; prev=%d got=%d min=%d",
				prevTarget, got, exec.minWorkerCount)
		}
	})

	// Verify CooldownUp prevents back-to-back upscales, and that upscaling resumes after cooldown.
	t.Run("CooldownUp_SuppressesRapidUpscale", func(t *testing.T) {
		exec := newExec(1)
		defer exec.Stop()

		// Configure scaler to step up by 1 with a non-zero cooldown.
		exec.poolScaler.cfg.MaxStepUp = 1
		exec.poolScaler.cfg.CooldownUp = 100 * time.Millisecond
		// Avoid incidental periodic downscales during the test window.
		exec.poolScaler.cfg.CooldownDown = time.Hour
		exec.workerPool.downScaleMinInterval = time.Hour

		prev := exec.workerPool.workerCountTarget.Load()

		// First upscale: immediate pressure requests more workers now.
		exec.poolScaler.scale(time.Now(), 8)
		// Wait until target advances by exactly 1 (MaxStepUp=1)
		deadline := time.Now().Add(50 * time.Millisecond)
		var afterFirst int32
		for time.Now().Before(deadline) {
			afterFirst = exec.workerPool.workerCountTarget.Load()
			if afterFirst == prev+1 {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		require.Equal(t, prev+1, afterFirst, "MaxStepUp=1 should advance target by 1")

		// Second upscale within cooldown should be suppressed.
		exec.poolScaler.scale(time.Now(), 8)
		// Brief wait to allow any (incorrect) change to manifest; then assert unchanged.
		time.Sleep(50 * time.Millisecond)
		afterSecond := exec.workerPool.workerCountTarget.Load()
		require.Equal(t, afterFirst, afterSecond, "second upscale should be blocked by CooldownUp")

		// After cooldown, another upscale should be applied.
		time.Sleep(exec.poolScaler.cfg.CooldownUp + 50*time.Millisecond)
		exec.poolScaler.scale(time.Now(), 8)
		deadline = time.Now().Add(750 * time.Millisecond)
		var afterThird int32
		for time.Now().Before(deadline) {
			afterThird = exec.workerPool.workerCountTarget.Load()
			if afterThird == afterSecond+1 {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		require.Equal(t, afterSecond+1, afterThird, "upscale should resume after CooldownUp")
	})

	// Verify that when target downscales, workers are actually stopped (runningWorkers decreases).
	t.Run("Downscale_StopsWorkers", func(t *testing.T) {
		exec := newExec(1)
		defer exec.Stop()

		// Make downscale permissive and fast.
		exec.poolScaler.cfg.CooldownDown = 0
		exec.poolScaler.cfg.MaxStepDown = 1000
		exec.workerPool.utilizationThreshold = 0.99
		exec.workerPool.downScaleMinInterval = 0

		// Upscale to several workers using immediate pressure.
		exec.poolScaler.scale(time.Now(), 4)
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			if exec.workerPool.runningWorkers() >= 4 {
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
		runningBefore := exec.workerPool.runningWorkers()
		require.GreaterOrEqual(t, runningBefore, int32(4), "expected workers to scale up before testing downscale")

		// Request downscale to min and wait for workers to stop.
		exec.poolScaler.scale(time.Now(), 0)
		deadline = time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			if exec.workerPool.runningWorkers() <= int32(exec.minWorkerCount) {
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
		runningAfter := exec.workerPool.runningWorkers()
		require.Equal(t, int32(exec.minWorkerCount), runningAfter, "expected workers to stop down to min")
		require.Less(t, runningAfter, runningBefore, "expected running workers to decrease")
	})
}

func TestPoolExecutor_WorkerPoolPeriodicScaling(t *testing.T) {
	// Start a manager with 1 worker and a short scaling interval.
	exec := newPoolExecutor(
		context.Background(),
		zerolog.New(zerolog.ConsoleWriter{Out: os.Stdout}),
		make(chan error),
		&executorMetrics{},
		4,
		1,
		20*time.Millisecond,
		PoolScaleConfig{
			MinWorkers:          1,
			MaxWorkers:          defaultMaxWorkerCount,
			TargetUtilization:   0.70,
			DeadbandRatio:       0,
			CooldownUp:          0,                      // fast up
			CooldownDown:        200 * time.Millisecond, // slow/cautious down
			MaxStepUp:           0,                      // jump to target
			MaxStepDown:         1,                      // gentle down
			EWMAFastAlpha:       0.5,
			EWMASlowAlpha:       0.1,
			BurstHeadroomFactor: 1.25,
		},
	)
	defer exec.Stop()
	exec.Start()
	time.Sleep(5 * time.Millisecond)

	// Add a job with ~4 x longer execution than cadence, resulting in at least 4 workers being
	// needed although only one additional will be added from scaling based on the job width
	job := Job{
		ID:       "test-periodic-scaling",
		Cadence:  5 * time.Millisecond,
		NextExec: time.Now().Add(20 * time.Millisecond),
		Tasks: []Task{MockTask{ID: "task1", executeFunc: func() error {
			testLogger.Debug().Msg("Executing task1")
			time.Sleep(25 * time.Millisecond) // Simulate 25 ms execution time
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

	time.Sleep(75 * time.Millisecond) // Allow time for periodic scaling to occur

	assert.GreaterOrEqual(
		t,
		exec.workerPool.targetWorkerCount(),
		int32(4),
		"Expected target worker count to be greater or equal than 4",
	)
}
