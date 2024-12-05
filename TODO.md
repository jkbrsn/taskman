# to-do list for the scheduler package

## to implement

- make tasks have ID:s + add an option to remove a task based on its ID
- an option to execute a job directly when inserted and after that at the regular cadence
- an option to execute a job only once, e.g. a "one-hit" job, either with immediate or delayed execution
- dynamic scaleup and scaledown of the number of workers
- move resultChan close to the scheduler, but add a signal from the worker pool to let the scheduler know it's done closing workers
- tests for worker_pool.go

# feature ideas

- Cron-like expressions for scheduling jobs. This would allow for more complex scheduling patterns than just a simple interval.
- Custom consumers for jobs. If the same app wants to run jobs in the same pool that are different enough that they require different consumers, the app should be able to provide the option to have a custom consumer for each job.
-A broadcast function, with a fan-out pattern, to send results to multiple channels in parallel.
