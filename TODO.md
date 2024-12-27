# to-do list for the go-taskman package

## to do before 0.1.0

- better readme
- clear in-code TODO:s
- ensure basic test coverage
- align function docs and error returns across package

## to do later

- dynamic scaleup and scaledown of the number of workers
  - add a dispatcher method to calculate a recommended worker count based on the job queue, taking into account the "widest" jobs and job frequency
    - the pool should probably hold at least 2 x the number of workers needed to handle the widest job, but look up a formula to calculate this
  - add a method to add workers to the pool post start
  - add a method to remove workers from the pool
- add an option to execute a job directly when inserted and after that at the regular cadence
- add an option to execute a job only once, e.g. a "one-hit" job, either with immediate or delayed execution
- add an option to instantly execute a job in the queue, even though it has some time until next execution
  - use heap.Fix to reposition the job in the heap, https://cs.opensource.google/go/go/+/refs/tags/go1.23.4:src/container/heap/heap.go;l=83

# feature ideas

- Task control
  - Make tasks within grouped jobs have ID:s + add an option to remove a task from a job based on its ID
  - Attach a context to a task, so that it can be cancelled and controlled in other ways
- Cron-like expressions for scheduling jobs. This would allow for more complex scheduling patterns than just a simple interval.
- Custom consumers for jobs. If the same app wants to run jobs in the same pool that are different enough that they require different consumers, the app should be able to provide the option to have a custom consumer for each job.
-A broadcast function, with a fan-out pattern, to send results to multiple channels in parallel.
