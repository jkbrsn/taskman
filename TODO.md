# to-do list for the go-taskman package

## to implement

- make AddJob take a Job instead of Task[]
- add helper ReplaceJob which wraps RemoveJob + AddJob
- add an option to execute a job directly when inserted and after that at the regular cadence
- add an option to execute a job only once, e.g. a "one-hit" job, either with immediate or delayed execution
- dynamic scaleup and scaledown of the number of workers
- resultChan
  - move resultChan close to the dispatcher, but add a signal from the worker pool to let the dispatcher know it's done closing workers
  - OR remove it entirely, let Execute return error, let the user handle results and errors
- worker pool
  - tests for worker_pool.go
- better readme

# feature ideas

- Task control
  - Make tasks within grouped jobs have ID:s + add an option to remove a task from a job based on its ID
  - Attach a context to a task, so that it can be cancelled and controlled in other ways
- Cron-like expressions for scheduling jobs. This would allow for more complex scheduling patterns than just a simple interval.
- Custom consumers for jobs. If the same app wants to run jobs in the same pool that are different enough that they require different consumers, the app should be able to provide the option to have a custom consumer for each job.
-A broadcast function, with a fan-out pattern, to send results to multiple channels in parallel.
