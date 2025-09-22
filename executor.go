package taskman

import "errors"

var (
	ErrExecutorContextDone = errors.New("executor context is done")
)

// executor is an interface for task executors.
type executor interface {
	Start()
	Stop()

	Schedule(job Job) error
	Remove(jobID string) error
	Replace(job Job) error

	Job(jobID string) (Job, error)
	Metrics() TaskManagerMetrics
}
