package taskman

import (
	"testing"

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
