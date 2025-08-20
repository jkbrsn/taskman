# go-taskman [![Go Documentation](http://img.shields.io/badge/go-documentation-blue.svg?style=flat-square)][godocs]

[godocs]: http://godoc.org/github.com/jkbrsn/go-taskman

An efficient and scalable task manager for in-process task scheduling in Go applications. The package is designed to handle a large number of concurrently running recurring jobs, while at the same time keeping the number of goroutines relatively low. A second design focus is the ability of simultaneous task execution, achieved by grouping of tasks into jobs.

**Features**

- Defines the interface `Task`, which when implemented allows for easy inclusion of existing structures in the manager.
- Grouping of tasks into `Job`s for near-simultaneous execution.
- Utilizes a worker pool setup.
  - This allows the manager to limit the number of spawned goroutines to the number of workers in the pool, and thus keeping memory usage down.
  - A priority queue is used to dispatch jobs for execution in the worker pool. The queue is a min heap, minimized by shortest time until next execution.
- Dynamic worker pool scaling.
  - The worker pool scales based on the state of the queue;
    - Largest parallel execution of tasks
    - Tasks executed per second
    - Average task execution time
  - The scaling algorithm is designed to optimize for worker availability, and as such errs on the safe side when it comes to scaling down.

## Install

```
go get github.com/jkbrsn/go-taskman
```

## Usage

### Basic usage

The most basic usage is to add a function directly, along with the cadence that the function execution should recurr at. In this case, a `jobID` is returned to allow the caller to later modify or remove the job.

```go
manager := New()
defer manager.Stop()

jobID, err := manager.ScheduleFunc(
    func() error {
        fmt.Println("Executing the function")
        return nil
    },
    10 * time.Second,
)
// Handle the error and do something with the job ID
```

### Advanced usage

Full usage of the package involves implementing the `Task` interface, adding tasks to the manager in a `Job`, and tweaking the manager parameters to your liking using the functional options.

```go
// Make an arbitrary struct implement the Task interface
type SomeStruct struct {
	ID      string
}

func (s SomeStruct) Execute() error {
	fmt.Printf("Executing SomeStruct with ID: %s", s.ID)
	return nil
}

...

// Utilize the implementation when adding a Job
manager := New(
  WithMinWorkerCount(4),
  WithChannelSize(16),
)
defer manager.Stop()

// A job with two tasks and a cadence of 10 seconds, set to have its first execution immediately
job := Job{
    Cadence:  10 * time.Second,
    ID:       "job1",
    NextExec: time.Now(),
    Tasks:    []Task{
        SomeStruct{ID: "task1"},
        SomeStruct{ID: "task2"},
    },
}

err := manager.ScheduleJob(job)
// Handle the error
```

### Logging

The package uses `zerolog` for logging purposes. By default, the package will initialize a no-op logger, but if logging from the task manager is desired the `WithLogger` option can be used to set a custom logger at construction.

```go
// Set a custom logger
manager := New(
    WithLogger(
        zerolog.New(os.Stdout).With().Timestamp().Logger(),
    ),
)
```

## Contributing

For contributions, please open a GitHub issue with your questions and suggestions. Before submitting an issue, have a look at the existing [TODO list](TODO.md) to see if your idea is already in the works.
