# go-taskman

An efficient and scalable task manager for in-process task scheduling in Go applications. The package is designed to handle a large number of concurrently running recurring jobs, while keeping the number of goroutines down. Another main focus is the ability to execute tasks in groups, which in the context of this manager is called a job.

**Features**

- Defines the interface `Task`, which when implemented allows for easy inclusion of existing structs in the task manager. Tasks can also be added without implementing the interface.
- Grouping of tasks into jobs for simultaneous execution. A "solo" recurring task is simply a `Job` with a single `Task` to execute.
- Utilizes a worker pool setup, limiting the number of goroutines to the number of workers in the pool, where the manager uses a priority queue to dispatch tasks for execution among the workers. The job queue is a min heap, sorted based on min time until next execution.

## Install

```
go get github.com/jakobilobi/go-taskman
```

## Usage

The most basic usage is to add functions directly, with the cadence that function should recurr at. In this case, a `jobID` is returned to allow the caller to later modify or remove the job.

```go
manager := NewManager()
defer manager.Stop()

jobID, err := manager.ScheduleFunc(
    func() error {
        log.Printf("Executing the function")
        return nil
    },
    10 * time.Second,
)
// Handle the err and do something with the job ID
```

Full usage of the package involves implementing the `Task` interface and adding tasks to the manager in `Job`s.

```go
// Make a select struct implement the Task interface
type SomeTask struct {
	ID      string
}

func (st SomeTask) Execute() error {
	log.Printf("Executing SomeTask with ID: %s", st.ID)
	return nil
}

// Utilize the implementation when adding a Job
manager := NewManager()
defer manager.Stop()

job := Job{
    Cadence:  10 * time.Second,
    ID:       "job1",
    NextExec: time.Now().Add(10 * time.Second),
    Tasks:    []Task{
        SomeTask{ID: "task1"},
        SomeTask{ID: "task2"},
    },
}

err := manager.ScheduleJob(job)
// Handle the err
```

## Contributing

For contributions, please open a GitHub issue with your questions and suggestions. Before submitting an issue, have a look at the existing [TODO list](TODO.md) to see if your idea is already in the works.
