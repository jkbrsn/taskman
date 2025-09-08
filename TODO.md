# Plans for the go-taskman package

## TODO v0.5.0

- Update module name to reflect new name `taskman` (from `go-taskman`)
- CI/CD
  - Add GitHub Actions CI/CD pipeline
  - Add GitHub Actions release pipeline
- Improve unit tests
  - Add test suite setup to `manager_test.go`, to allow for testing of both executor types
- Add more functional options
  - An option to execute a job a select amount of times, e.g. a "one-hit" or "multi-hit" job, either with immediate or delayed execution
  - An option to instantly execute a job already in the queue, even though it has some time until next execution
    - use heap.Fix to reposition the job in the heap, https://cs.opensource.google/go/go/+/refs/tags/go1.23.4:src/container/heap/heap.go;l=83
  - An option to pause/stop a job
    - point would be to not have to remove a job and reinsert it when it should be resumed
    - could internally involve removing it from the queue, to an separate slice/structure, and then reinserting it when it should be resumed
- Metrics
  - Consider adding metrics for channel buffer sizes and queue sizes
  - Consider ingesting executorMetrics updates via channel to avoid locks

# feature ideas

- Task control
  - Make tasks within grouped jobs have ID:s + add an option to remove a task from a job based on its ID
  - Attach a context to a task, so that it can be cancelled and controlled in other ways
- Cron-like expressions for scheduling jobs. This would allow for more complex scheduling patterns than just a simple interval.
- Custom consumers for jobs. If the same app wants to run jobs in the same pool that are different enough that they require different consumers, the app should be able to provide the option to have a custom consumer for each job.
- A broadcast function, with a fan-out pattern, to send results to multiple channels in parallel.
