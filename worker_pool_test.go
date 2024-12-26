package taskman

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewWorkerPool(t *testing.T) {
	resultChan := make(chan Result, 1)
	taskChan := make(chan Task, 1)
	pool := newWorkerPool(10, resultChan, taskChan)
	defer pool.stop()

	// Verify stopChan
	assert.NotNil(t, pool.stopChan, "Expected stop channel to be non-nil")
}

func TestWorkerPoolStart(t *testing.T) {
	resultChan := make(chan Result, 1)
	taskChan := make(chan Task, 1)
	pool := newWorkerPool(10, resultChan, taskChan)
	defer func() {
		pool.stop()

		// Verify worker counts post-stop
		assert.Equal(t, 10, pool.workersTotal, "Expected worker count to be 10")
		assert.Equal(t, int32(0), pool.activeWorkers(), "Expected no active workers")
		assert.Equal(t, int32(0), pool.runningWorkers(), "Expected no running workers")
	}()

	// Verify worker counts pre-start
	assert.Equal(t, 10, pool.workersTotal, "Expected worker count to be 10")
	assert.Equal(t, int32(0), pool.activeWorkers(), "Expected no active workers")
	assert.Equal(t, int32(0), pool.runningWorkers(), "Expected no running workers")

	pool.start()
	time.Sleep(20 * time.Millisecond) // Wait for workers to start

	// Verify worker counts post-start
	assert.Equal(t, 10, pool.workersTotal, "Expected worker count to be 10")
	assert.Equal(t, int32(0), pool.activeWorkers(), "Expected no active workers")
	assert.Equal(t, int32(10), pool.runningWorkers(), "Expected 10 running workers")
}
