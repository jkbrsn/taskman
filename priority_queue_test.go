package taskman

import (
	"container/heap"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestPushAndPop(t *testing.T) {
	pq := &priorityQueue{}
	heap.Init(pq)

	job1 := &Job{ID: "job1", NextExec: time.Now().Add(10 * time.Second)} // Should be popped second
	job2 := &Job{ID: "job2", NextExec: time.Now().Add(5 * time.Second)}  // Should be popped first
	job3 := &Job{ID: "job3", NextExec: time.Now().Add(15 * time.Second)} // Should be popped last

	heap.Push(pq, job1)
	heap.Push(pq, job2)
	heap.Push(pq, job3)

	assert.Equal(t, "job2", heap.Pop(pq).(*Job).ID, "Expected job2 to be popped first")
	assert.Equal(t, "job1", heap.Pop(pq).(*Job).ID, "Expected job1 to be popped second")
	assert.Equal(t, "job3", heap.Pop(pq).(*Job).ID, "Expected job3 to be popped last")
}

func TestJobInQueue(t *testing.T) {
	pq := &priorityQueue{}
	heap.Init(pq)

	job := &Job{ID: "job1", NextExec: time.Now().Add(10 * time.Second)}
	heap.Push(pq, job)

	index, err := pq.JobInQueue("job1")
	assert.Nil(t, err, "Error when checking for job1 in queue")
	assert.Equal(t, 0, index, "Expected job1 to be found at index 0")

	_, err = pq.JobInQueue("job2")
	assert.ErrorIs(t, err, ErrJobNotFound, "Expected job2 to not be found")
}

func TestPeek(t *testing.T) {
	pq := &priorityQueue{}
	heap.Init(pq)

	job1 := &Job{ID: "job1", NextExec: time.Now().Add(10 * time.Second)}
	job2 := &Job{ID: "job2", NextExec: time.Now().Add(5 * time.Second)}
	heap.Push(pq, job1)
	heap.Push(pq, job2)

	assert.Equal(t, "job2", pq.Peek().ID, "Expected job2 to be peeked first")
}

func TestRemoveByID(t *testing.T) {
	pq := &priorityQueue{}
	heap.Init(pq)

	job1 := &Job{ID: "job1", NextExec: time.Now().Add(10 * time.Second)}
	job2 := &Job{ID: "job2", NextExec: time.Now().Add(5 * time.Second)}
	heap.Push(pq, job1)
	heap.Push(pq, job2)

	err := pq.RemoveByID("job1")
	assert.Nil(t, err, "Error when removing job1")

	_, err = pq.JobInQueue("job1")
	assert.ErrorIs(t, err, ErrJobNotFound, "Expected job1 to not be found after removal")

	assert.Equal(t, 1, pq.Len(), "Expected queue length to be 1 after removal")
}

func TestUpdate(t *testing.T) {
	pq := &priorityQueue{}
	heap.Init(pq)

	job1 := &Job{ID: "job1", NextExec: time.Now().Add(10 * time.Second)}
	job2 := &Job{ID: "job2", NextExec: time.Now().Add(5 * time.Second)}
	heap.Push(pq, job1)
	heap.Push(pq, job2)

	// Update job1 to have an earlier NextExec time than job2
	newNextExec := time.Now().Add(1 * time.Second)
	// Call Update to heap.Fix the queue, placing job1 on top
	pq.Update(job1, newNextExec)

	assert.Equal(t, "job1", pq.Peek().ID, "Expected job1 to be peeked first after update")
	assert.Equal(t, newNextExec, pq.Peek().NextExec, "Expected job1 to have updated NextExec time")
}
