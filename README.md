# taskman [![Go Documentation](http://img.shields.io/badge/go-documentation-blue.svg?style=flat-square)][godocs] [![Go Version](https://img.shields.io/badge/go-1.25.1-blue.svg)](https://golang.org/dl/) [![CI](https://github.com/jkbrsn/taskman/actions/workflows/ci.yml/badge.svg)](https://github.com/jkbrsn/taskman/actions/workflows/ci.yml) [![Latest Release](https://img.shields.io/github/v/release/jkbrsn/taskman)](https://github.com/jkbrsn/taskman/releases)

[godocs]: http://godoc.org/github.com/jkbrsn/taskman

An efficient and scalable task manager for in-process task scheduling in Go applications. The package is designed to handle a large number of concurrently running recurring jobs, while at the same time keeping the number of goroutines relatively low. A second design focus is the ability of simultaneous task execution, achieved by grouping of tasks into jobs.

**Features**

- Defines the interface `Task`, which when implemented allows for easy inclusion of existing structures in the manager.
- Grouping of tasks into `Job`s for near-simultaneous execution.
- Multiple execution modes:
  - **Pool Mode** (default): Utilizes a worker pool setup with dynamic scaling.
    - Keeps memory usage down by limiting the number of goroutines to the number of workers in the pool.
    - Dynamic worker pool scaling using a control loop with exponentially weighted moving averages (EWMA) for tasks per second and execution times, targeting 70% utilization with a 10% deadband, and cooldown periods to prevent thrashing.
  - **Distributed Mode**: Each job runs as its own long-lived goroutine with configurable parallelism and catch-up behavior.
  - **On-Demand Mode**: Hybrid approach using a priority queue but spawning short-lived goroutines per job execution.

## Install

```
go get github.com/jkbrsn/taskman
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
  WithMode(ModePool), // or ModeDistributed, ModeOnDemand
  WithMPMinWorkerCount(4), // Pool mode options
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

The package uses `zerolog` for logging purposes. It defaults to a no-op logger, but an initialized logger can be injected using the `WithLogger` option.

```go
// Set a custom logger
manager := New(
    WithLogger(
        zerolog.New(os.Stdout).With().Timestamp().Logger(),
    ),
)
```

### Metrics

The package provides comprehensive metrics about task manager performance. Metrics can be retrieved at any time using the `Metrics()` method, which returns a `TaskManagerMetrics` struct containing:

- **Jobs**: ManagedJobs (total jobs), JobsPerSecond, JobsTotalExecutions
- **Tasks**: ManagedTasks (total tasks), TasksPerSecond, TasksAverageExecTime, TasksTotalExecutions
- **Worker Pool**: PoolMetrics (for Pool Mode, includes worker count, utilization, etc.)

```go
metrics := manager.Metrics()
fmt.Printf("Jobs per second: %.2f\n", metrics.JobsPerSecond)
fmt.Printf("Tasks average exec time: %v\n", metrics.TasksAverageExecTime)
```

## Contributing

For contributions, please open a GitHub issue with your questions and suggestions. Before submitting an issue, have a look at the existing [TODO list](TODO.md) to see if your idea is already in the works.
