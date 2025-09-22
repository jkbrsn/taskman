# Plans for the go-taskman package

## TODO v0.6.0

- Add a functional option to execute a job a select amount of times, e.g. a "one-hit" or "multi-hit" job, either with immediate or delayed execution
- Add a function to instantly execute a job already in the queue, even though it has some time until next execution
  - use heap.Fix to reposition the job in the heap, https://cs.opensource.google/go/go/+/refs/tags/go1.23.4:src/container/heap/heap.go;l=83
- Add Task Execution Timeouts: Implement per-task timeouts to prevent indefinite hangs, using context.WithTimeout in worker goroutines and propagating deadline exceeded errors.

## Future ideas

- Task control
  - Make tasks within grouped jobs have ID:s + add an option to remove a task from a job based on its ID
  - Attach a context to a task, so that it can be cancelled and controlled in other ways
- Cron-like expressions for scheduling jobs. This would allow for more complex scheduling patterns than just a simple interval.
- Custom consumers for jobs. If the same app wants to run jobs in the same pool that are different enough that they require different consumers, the app should be able to provide the option to have a custom consumer for each job.
- A broadcast function, with a fan-out pattern, to send results to multiple channels in parallel.
- Retry Logic with Backoff: Add configurable retry attempts for transient failures, using exponential backoff to avoid overwhelming downstream systems.
