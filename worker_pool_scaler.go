package taskman

import (
	"math"
	"time"

	"github.com/rs/zerolog"
)

// PoolScaleConfig tunes the worker pool control loop.
type PoolScaleConfig struct {
	MinWorkers        int
	MaxWorkers        int
	TargetUtilization float64       // e.g. 0.70
	DeadbandRatio     float64       // e.g. 0.10 (±10% around target)
	CooldownUp        time.Duration // e.g. 2 * time.Second
	CooldownDown      time.Duration // e.g. 30 * time.Second
	MaxStepUp         int           // e.g. 0 => jump to target, else cap increase per decision
	MaxStepDown       int           // e.g. 1–2 => gentle downscale
	EWMAFastAlpha     float64       // e.g. 0.4
	EWMASlowAlpha     float64       // e.g. 0.1

	// Optional safety headroom when backlog is detected.
	BurstHeadroomFactor float64 // e.g. 1.25 (25% extra when we detect immediate pressure)
}

// poolScaler keeps controller state between ticks.
type poolScaler struct {
	log zerolog.Logger
	cfg PoolScaleConfig

	workerPool *workerPool
	metrics    *managerMetrics

	lastScaleUp   time.Time
	lastScaleDown time.Time

	// Smoothed signals (dual horizon).
	lambdaFast float64 // tasks/sec (fast EWMA)
	lambdaSlow float64 // tasks/sec (slow EWMA)
	esFast     float64 // mean exec seconds (fast EWMA)
	esSlow     float64 // mean exec seconds (slow EWMA)
}

// scale implements a control-loop with EWMA smoothing, target utilization,
// hysteresis/deadband, and asymmetric (fast-up, slow-down) behavior.
func (s *poolScaler) scale(
	now time.Time,
	workersNeededNow int, // immediate pressure signal from your scheduler
) {
	available := int(s.workerPool.availableWorkers())
	running := int(s.workerPool.runningWorkers())
	prevTarget := int(s.workerPool.workerCountTarget.Load())
	s.log.Debug().Msgf("Scaling workers, available/running: %d/%d", available, running)

	// 1) Read instantaneous signals (cheap, already tracked in your metrics)
	instLambda := float64(s.metrics.tasksPerSecond.Load()) // tasks/sec
	instESec := s.metrics.averageExecTime.Load().Seconds() // sec per task
	if instESec < 1e-6 {
		// Avoid divide-by-zero; if we have no recent tasks, assume tiny service time.
		instESec = 1e-6
	}

	// 2) Update dual-horizon EWMAs
	// If no tasks are managed, aggressively decay lambdas to zero.
	if s.metrics.tasksManaged.Load() == 0 {
		instLambda = 0
	}

	s.lambdaFast = ewmaUpdate(s.lambdaFast, instLambda, s.cfg.EWMAFastAlpha)
	s.lambdaSlow = ewmaUpdate(s.lambdaSlow, instLambda, s.cfg.EWMASlowAlpha)
	s.esFast = ewmaUpdate(s.esFast, instESec, s.cfg.EWMAFastAlpha)
	s.esSlow = ewmaUpdate(s.esSlow, instESec, s.cfg.EWMASlowAlpha)

	// Blend fast/slow to get a stable but responsive estimate.
	// Weight the fast more when load is rising (simple heuristic).
	rising := s.lambdaFast > s.lambdaSlow*1.02
	var lambdaHat, eSHat float64
	if rising {
		lambdaHat = 0.7*s.lambdaFast + 0.3*s.lambdaSlow
		eSHat = 0.7*s.esFast + 0.3*s.esSlow
	} else {
		lambdaHat = 0.4*s.lambdaFast + 0.6*s.lambdaSlow
		eSHat = 0.4*s.esFast + 0.6*s.esSlow
	}

	// 3) Compute demand-based worker target from utilization setpoint
	// Little’s Law-ish sizing: Needed ≈ ceil( (λ * E[S]) / target_util ).
	if s.cfg.TargetUtilization <= 0 || s.cfg.TargetUtilization > 0.9 {
		// sane defaults
		s.cfg.TargetUtilization = 0.70
	}
	demandWorkers := int(math.Ceil((lambdaHat * eSHat) / s.cfg.TargetUtilization))
	// If there is no managed work, prefer drifting toward minimum.
	if s.metrics.tasksManaged.Load() == 0 {
		demandWorkers = s.cfg.MinWorkers
	}
	s.log.Debug().Msgf("Demand-based workers: %d", demandWorkers)

	// 4) Compute immediate pressure worker target (burst/backlog)
	// If scheduler says we need more *right now* than we have available, propose enough workers
	// to cover it, with optional burst headroom.
	// Default immediate target assumes no special pressure.
	immediate := 0
	// Treat equality as pressure: if needed >= available, cover the gap with headroom.
	if workersNeededNow >= available {
		// Ensure target covers currently running plus the shortfall (needed - available).
		shortfall := max(workersNeededNow-available, 0)
		base := running + shortfall
		immediate = int(math.Ceil(float64(base) * math.Max(s.cfg.BurstHeadroomFactor, 1.0)))
	}
	s.log.Debug().Msgf("Immediate pressure: %d", immediate)

	// 5) Combine signals and clamp. Always ensure we can cover workersNeededNow immediately.
	desired := max(immediate, demandWorkers)
	desired = clampInt(desired, s.cfg.MinWorkers, s.cfg.MaxWorkers)
	s.log.Debug().Msgf("Desired workers after clamp: %d", desired)

	// 6) Deadband / hysteresis around previous target worker count
	// Convert % deadband to absolute units w.r.t prev.
	deadband := int(math.Ceil(float64(prevTarget) * math.Max(s.cfg.DeadbandRatio, 0)))
	// Allow tests/config to set zero deadband; only enforce min=1 if ratio > 0.
	if s.cfg.DeadbandRatio > 0 && deadband < 1 {
		deadband = 1
	}
	// Respect immediate pressure: if workersNeededNow is non-zero, bypass deadband for upscales.
	// Also, when there is no managed work, bypass deadband for downscales toward min.
	if !((workersNeededNow > 0 && desired > prevTarget) || (s.metrics.tasksManaged.Load() == 0 && desired < prevTarget)) {
		if absInt(desired-prevTarget) <= deadband {
			s.log.Debug().Msgf("Within deadband: suppress change")
			return
		}
	}

	//  7) Asymmetric cooldowns + step caps
	nowIsAfter := func(t time.Time, d time.Duration) bool {
		return t.IsZero() || now.Sub(t) >= d
	}

	var next int
	if desired > prevTarget {
		// Scale UP: fast path (short/no cooldown), optionally cap the jump.
		if s.cfg.CooldownUp > 0 && !nowIsAfter(s.lastScaleUp, s.cfg.CooldownUp) {
			// Too soon to scale up again.
			return
		}
		if s.cfg.MaxStepUp > 0 {
			next = prevTarget + min(s.cfg.MaxStepUp, desired-prevTarget)
		} else {
			next = desired // jump
		}
		s.lastScaleUp = now
	} else { // desired < prev
		// Scale DOWN: cautious path (long cooldown), small steps.
		if s.cfg.CooldownDown > 0 && !nowIsAfter(s.lastScaleDown, s.cfg.CooldownDown) {
			return
		}
		step := s.cfg.MaxStepDown
		if step <= 0 {
			step = 1
		}
		next = prevTarget - min(step, prevTarget-desired)
		s.lastScaleDown = now
	}
	s.log.Debug().Msgf("Desired workers after cooldowns and caps: %d", next)

	// Final clamp and enqueue.
	next = clampInt(next, s.cfg.MinWorkers, s.cfg.MaxWorkers)
	if next == prevTarget {
		return
	}

	s.workerPool.enqueueWorkerScaling(int32(next))

	s.log.Debug().
		Int("prev", prevTarget).
		Int("next", next).
		Int("desired", desired).
		Float64("lambda_fast", s.lambdaFast).
		Float64("lambda_slow", s.lambdaSlow).
		Float64("E[S]_fast", s.esFast).
		Float64("E[S]_slow", s.esSlow).
		Float64("target_util", s.cfg.TargetUtilization).
		Float64("deadband_ratio", s.cfg.DeadbandRatio).
		Msg("autoscale result")
}

// defaultPoolScaleCfg returns the default pool scale configuration.
func defaultPoolScaleCfg() PoolScaleConfig {
	return PoolScaleConfig{
		MaxWorkers:          defaultMaxWorkerCount,
		MinWorkers:          defaultMinWorkerCount,
		TargetUtilization:   0.70,
		DeadbandRatio:       0.10,
		CooldownUp:          2 * time.Second,
		CooldownDown:        45 * time.Second,
		MaxStepUp:           0, // 0 = no cap (jump)
		MaxStepDown:         1,
		EWMAFastAlpha:       0.4,
		EWMASlowAlpha:       0.10,
		BurstHeadroomFactor: 1.25,
	}

}

// newPoolScaler creates a new pool scaler.
func newPoolScaler(
	logger zerolog.Logger,
	workerPool *workerPool,
	metrics *managerMetrics,
	cfg PoolScaleConfig,
) *poolScaler {
	log := logger.With().Str("component", "pool_scaler").Logger()
	return &poolScaler{
		log:           log,
		cfg:           cfg,
		workerPool:    workerPool,
		metrics:       metrics,
		lastScaleUp:   time.Now(),
		lastScaleDown: time.Now(),
		lambdaFast:    0,
		lambdaSlow:    0,
		esFast:        0,
		esSlow:        0,
	}
}

// HELPERS

func absInt(x int) int {
	if x < 0 {
		return -x
	}
	return x
}

func clampInt(v, lo, hi int) int {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

func ewmaUpdate(prev, sample, alpha float64) float64 {
	if prev == 0 {
		// First sample bootstrap.
		return sample
	}
	return alpha*sample + (1-alpha)*prev
}
