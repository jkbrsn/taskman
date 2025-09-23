# Plans for the go-taskman package

## TODO v0.7.0

- Task control
  - (Potentially) Make tasks within grouped jobs have ID:s + add an option to remove a task from a job based on its ID
  - Add `context.Context` to tasks, to allow for timeouts and cancellation of tasks.

## Future ideas

- Add a function to instantly execute a job already in the queue, even though it has some time until next execution
  - use heap.Fix to reposition the job in the heap, https://cs.opensource.google/go/go/+/refs/tags/go1.23.4:src/container/heap/heap.go;l=83
- Cron-like expressions for scheduling jobs. This would allow for more complex scheduling patterns than just a simple interval.
- Custom consumers for jobs. If the same app wants to run jobs in the same pool that are different enough that they require different consumers, the app should be able to provide the option to have a custom consumer for each job.
- A broadcast function, with a fan-out pattern, to send results to multiple channels in parallel.
- Retry Logic with Backoff: Add configurable retry attempts for transient failures, using exponential backoff to avoid overwhelming downstream systems.
