package taskman

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestJobValidate(t *testing.T) {
	t.Run("valid job", func(t *testing.T) {
		validJob := Job{
			ID:       "valid-job",
			Cadence:  100 * time.Millisecond,
			NextExec: time.Now().Add(100 * time.Millisecond),
			Tasks:    []Task{MockTask{ID: "task1"}},
		}
		assert.NoError(t, validJob.Validate(), "Expected no error for valid job")
	})

	t.Run("invalid job", func(t *testing.T) {
		invalidJobZeroCadence := Job{
			ID:       "invalid-job-zero-cadence",
			Cadence:  0,
			NextExec: time.Now().Add(100 * time.Millisecond),
			Tasks:    []Task{MockTask{ID: "task1"}},
		}
		assert.Error(t, invalidJobZeroCadence.Validate(),
			"Expected error for job with zero cadence")

		invalidJobNoTasks := Job{
			ID:       "invalid-job-no-tasks",
			Cadence:  100 * time.Millisecond,
			NextExec: time.Now().Add(100 * time.Millisecond),
			Tasks:    []Task{},
		}
		assert.Error(t, invalidJobNoTasks.Validate(), "Expected error for job with no tasks")

		invalidJobZeroNextExec := Job{
			ID:       "invalid-job-zero-next-exec",
			Cadence:  100 * time.Millisecond,
			NextExec: time.Time{},
			Tasks:    []Task{MockTask{ID: "task1"}},
		}
		assert.NoError(t, invalidJobZeroNextExec.Validate(),
			"Expected no error when NextExec is zero (unset)")
	})
}
