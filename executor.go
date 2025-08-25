// Package taskman provides a simple task scheduler with a worker pool.
package taskman

// executor is an interface for task executors.
type executor interface {
	Start()
	Stop()
	Schedule(job Job) error
	Remove(jobID string) error
	Replace(job Job) error
	Metrics() TaskManagerMetrics
}
