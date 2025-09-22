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

// newPoolExecutorForTest creates a fresh executor with tiny intervals for fast tests.
func newPoolExecutorForTest(minWorkers int) *poolExecutor {
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

func TestPoolExecutor_ScaleUpToDemand(t *testing.T) {
	exec := newPoolExecutorForTest(1)
	defer exec.Stop()

	// One task every 5ms, each runs ~20ms ⇒ λ≈200/s, E[S]≈0.02
	// Needed ≈ ceil(λ*E[S]/0.7) ≈ ceil(4/0.7) ≈ 6
	job := Job{
		ID:       "burst-demand",
		Cadence:  1 * time.Millisecond,
		NextExec: time.Now().Add(5 * time.Millisecond),
		Tasks: []Task{
			MockTask{ID: "t", executeFunc: func() error {
				time.Sleep(15 * time.Millisecond)
				return nil
			}},
		},
	}
	require.NoError(t, exec.Schedule(job))

	// Let a couple of executions happen so metrics (TPS, avg exec) move.
	time.Sleep(75 * time.Millisecond)

	prev := exec.workerPool.workerCountTarget.Load()
	// Force a scale tick using current instantaneous metrics.
	exec.poolScaler.scale(time.Now(), 0)
	time.Sleep(10 * time.Millisecond)

	got := exec.workerPool.workerCountTarget.Load()
	require.Greater(t, got, prev,
		"expected target workers to increase (prev=%d, got=%d)", prev, got)
}

func TestPoolExecutor_ImmediatePressureOverridesDemand(t *testing.T) {
	exec := newPoolExecutorForTest(1)
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
		t.Fatalf("expected target to increase and cover immediate need; "+
			"prev=%d running0=%d avail0=%d wantMin=%d got=%d",
			prev, running0, avail0, wantMin, got)
	}
}

func TestPoolExecutor_PauseResume(t *testing.T) {
	exec := newPoolExecutorForTest(1)
	defer exec.Stop()

	jobID := "pause-resume"
	taskExecutionTimes := make(chan time.Time, 1)

	initialDelay := 120 * time.Millisecond
	job := Job{
		ID:       jobID,
		Cadence:  250 * time.Millisecond,
		NextExec: time.Now().Add(initialDelay),
		Tasks: []Task{MockTask{ID: "pause-task", executeFunc: func() error {
			taskExecutionTimes <- time.Now()
			return nil
		}}},
	}

	require.NoError(t, exec.Schedule(job))
	time.Sleep(10 * time.Millisecond)

	state, err := exec.Job(jobID)
	require.NoError(t, err)
	require.True(t,
		state.NextExec.After(time.Now()),
		"job NextExec should be scheduled in the future")

	require.NoError(t, exec.Pause(jobID))

	exec.mu.RLock()
	pausedEntry, ok := exec.pausedJobs[jobID]
	exec.mu.RUnlock()
	require.True(t, ok, "job should be present in paused map after pause")
	remaining := pausedEntry.remaining
	require.Greater(t, remaining, time.Duration(0), "remaining delay must be positive when paused")

	select {
	case <-taskExecutionTimes:
		t.Fatal("task executed while job was paused")
	case <-time.After(remaining + 75*time.Millisecond):
	}

	resumeTime := time.Now()
	require.NoError(t, exec.Resume(jobID))

	var executedAt time.Time
	select {
	case executedAt = <-taskExecutionTimes:
	case <-time.After(remaining + 150*time.Millisecond):
		t.Fatal("task did not execute after resume")
	}

	elapsed := executedAt.Sub(resumeTime)
	const earlyTolerance = 30 * time.Millisecond
	const lateTolerance = 80 * time.Millisecond
	assert.GreaterOrEqual(t, elapsed, remaining-earlyTolerance,
		"job executed sooner than remaining delay after resume (want >= %s, got %s)",
		remaining-earlyTolerance, elapsed)
	assert.LessOrEqual(t, elapsed, remaining+lateTolerance,
		"job executed later than expected delay after resume (want <= %s, got %s)",
		remaining+lateTolerance, elapsed)

	exec.mu.RLock()
	_, stillPaused := exec.pausedJobs[jobID]
	exec.mu.RUnlock()
	assert.False(t, stillPaused, "job should be removed from paused map after resume")
}

func TestPoolExecutor_DeadbandSuppressesSmallFluctuations(t *testing.T) {
	exec := newPoolExecutorForTest(4)
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
}

func TestPoolExecutor_CapAtMaxWorkerCount(t *testing.T) {
	exec := newPoolExecutorForTest(1)
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
}

func TestPoolExecutor_ScaleDownAfterRemovingJobs(t *testing.T) {
	exec := newPoolExecutorForTest(1)
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
	time.Sleep(100 * time.Millisecond)
	exec.poolScaler.scale(time.Now(), 0)
	time.Sleep(20 * time.Millisecond)

	prevTarget := exec.workerPool.workerCountTarget.Load()

	// Remove the job and wait past downscale cooldown.
	require.NoError(t, exec.Remove("remove-me"))

	time.Sleep(exec.poolScaler.cfg.CooldownDown)
	exec.poolScaler.scale(time.Now(), 0)
	time.Sleep(20 * time.Millisecond)

	got := exec.workerPool.workerCountTarget.Load()
	if got >= prevTarget || got < int32(exec.minWorkerCount) {
		t.Fatalf("expected downscale towards min; prev=%d got=%d min=%d",
			prevTarget, got, exec.minWorkerCount)
	}
}

func TestPoolExecutor_CooldownUpSuppressesRapidUpscale(t *testing.T) {
	exec := newPoolExecutorForTest(1)
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
}

func TestPoolExecutor_DownscaleStopsWorkers(t *testing.T) {
	exec := newPoolExecutorForTest(1)
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
	require.GreaterOrEqual(t, runningBefore, int32(4),
		"expected workers to scale up before testing downscale")

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
	require.Equal(t, int32(exec.minWorkerCount), runningAfter,
		"expected workers to stop down to min")
	require.Less(t, runningAfter, runningBefore, "expected running workers to decrease")
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
