package taskman

import (
	"container/heap"
	"time"
)

// PriorityQueue implements heap.Interface and holds Jobs.
type PriorityQueue []*Job

func (pq PriorityQueue) Len() int { return len(pq) }

// Less prioritizes jobs with earlier NextExec times.
func (pq PriorityQueue) Less(i, j int) bool {
	return pq[i].NextExec.Before(pq[j].NextExec)
}

func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i // Maintain index within the heap.
	pq[j].index = j
}

// Push adds a job to the heap.
func (pq *PriorityQueue) Push(x interface{}) {
	n := len(*pq)
	job := x.(*Job)
	job.index = n
	*pq = append(*pq, job)
}

// Pop removes and returns the job with the earliest NextExec time.
func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	job := old[n-1]
	job.index = -1 // For safety.
	*pq = old[0 : n-1]
	return job
}

// Update modifies the NextExec of a job in the heap.
func (pq *PriorityQueue) Update(job *Job, nextExec time.Time) {
	job.NextExec = nextExec
	heap.Fix(pq, job.index)
}

// Peek returns the job with the earliest NextExec time.
// TODO: test
func (pq *PriorityQueue) Peek() *Job {
	if len(*pq) == 0 {
		return nil
	}
	return (*pq)[0]
}

// Remove removes a job from the heap.
// TODO: test
func (pq *PriorityQueue) Remove(job *Job) {
	heap.Remove(pq, job.index)
}
