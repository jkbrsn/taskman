# Plans for the go-taskman package

## TODO v0.2.0

- revisist default values of `NewManager`, e.g. channel buffer sizes
  - not super important, since dynamic scaling will override these values
- dynamic scaleup and scaledown of the number of workers
  - utilize task metrics in the manager to determine if more/less workers are needed
- clean up in-code todo:s

## TODO v0.3.0

- add an option to execute a job directly when inserted and after that at the regular cadence
  - may already be achievable by setting NextExec = time.Now but should be confirmed and documented
- add an option to execute a job only once, e.g. a "one-hit" job, either with immediate or delayed execution
- add an option to instantly execute a job in the queue, even though it has some time until next execution
  - use heap.Fix to reposition the job in the heap, https://cs.opensource.google/go/go/+/refs/tags/go1.23.4:src/container/heap/heap.go;l=83
- figure out how to handle panics in the worker goroutines
  - should the worker crash or recover?
  - should the job be marked as failed and be dumped?
  - or should the job be retried next cadence?
- add a method to pause/stop a job
  - could involve removing it from the queue, to an external list, and then reinserting it when it should be resumed

# feature ideas

- Task control
  - Make tasks within grouped jobs have ID:s + add an option to remove a task from a job based on its ID
  - Attach a context to a task, so that it can be cancelled and controlled in other ways
- Cron-like expressions for scheduling jobs. This would allow for more complex scheduling patterns than just a simple interval.
- Custom consumers for jobs. If the same app wants to run jobs in the same pool that are different enough that they require different consumers, the app should be able to provide the option to have a custom consumer for each job.
- A broadcast function, with a fan-out pattern, to send results to multiple channels in parallel.
- Mirror the priority queue contents in a map, avoiding having to touch the queue, and thus reducing number of accesses, for anything but Push Pop Fix Update.
  - Would require a pairing of map adjustments with any queue alteration, resulting in more maintenance.
