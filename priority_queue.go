package taskman

import (
	"container/heap"
	"time"
)

// priorityQueue implements heap.Interface and holds Jobs.
// Priority is determined by the NextExec time of the Job.
type priorityQueue []*Job

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
func (pq *priorityQueue) Push(x interface{}) {
	n := len(*pq)
	job := x.(*Job)
	job.index = n
	*pq = append(*pq, job)
}

// Pop removes and returns the job with the earliest NextExec time.
func (pq *priorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	job := old[n-1]
	job.index = -1 // For safety.
	*pq = old[0 : n-1]
	return job
}

// Update modifies the NextExec of a job in the heap.
func (pq *priorityQueue) Update(job *Job, nextExec time.Time) {
	job.NextExec = nextExec
	heap.Fix(pq, job.index)
}

// Peek returns the job with the earliest NextExec time.
// TODO: test
func (pq *priorityQueue) Peek() *Job {
	if len(*pq) == 0 {
		return nil
	}
	return (*pq)[0]
}

// RemoveByID finds a job in the priorityQueue by ID, and removes it if found.
// TODO: test
func (pq *priorityQueue) RemoveByID(jobID string) error {
	// Find the job in the heap and
	for i, job := range *pq {
		if job.ID == jobID {
			heap.Remove(pq, i)
			return nil
		}
	}
	return ErrJobNotFound
}
