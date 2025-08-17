package taskman

import (
	"container/heap"
	"errors"
	"time"
)

// priorityQueue implements heap.Interface and holds Jobs.
// Priority is determined by the NextExec time of the Job.
type priorityQueue []*Job

// Interface implementation

// Len returns the length of the heap.
func (pq priorityQueue) Len() int { return len(pq) }

// Less prioritizes jobs with earlier NextExec times.
func (pq priorityQueue) Less(i, j int) bool {
	return pq[i].NextExec.Before(pq[j].NextExec)
}

// Swap swaps two jobs in the heap.
func (pq priorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i // Maintain index within the heap.
	pq[j].index = j
}

// Push adds a job to the heap.
func (pq *priorityQueue) Push(x any) {
	n := len(*pq)
	job := x.(*Job)
	job.index = n
	*pq = append(*pq, job)
}

// Pop removes and returns the job with the earliest NextExec time.
func (pq *priorityQueue) Pop() any {
	old := *pq
	n := len(old)
	job := old[n-1]
	job.index = -1 // For safety.
	*pq = old[0 : n-1]
	return job
}

// Custom functionality

// JobInQueue finds whether a job with the jobID is currently in the queue, and returns the job's
// index if found.
func (pq *priorityQueue) JobInQueue(jobID string) (int, error) {
	for _, job := range *pq {
		if job.ID == jobID {
			return job.index, nil
		}
	}
	return 0, errors.New("job not found")
}

// Peek returns the job with the earliest NextExec time.
func (pq *priorityQueue) Peek() *Job {
	if len(*pq) == 0 {
		return nil
	}
	return (*pq)[0]
}

// RemoveByID finds a job in the priorityQueue by ID, and removes it if found.
func (pq *priorityQueue) RemoveByID(jobID string) error {
	for i, job := range *pq {
		if job.ID == jobID {
			heap.Remove(pq, i)
			return nil
		}
	}
	return errors.New("job not found")
}

// Update modifies the NextExec time of a job in the heap.
func (pq *priorityQueue) Update(job *Job, newNextExec time.Time) {
	job.NextExec = newNextExec
	heap.Fix(pq, job.index)
}
