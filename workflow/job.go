package workflow

import (
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"time"
)

type JobStatus int

const (
	Created JobStatus = iota
	Running
	Successed
	Failed
	Aborted
)

var job_statuses = []JobStatus{
	Created,
	Running,
	Successed,
	Failed,
	Aborted,
}

type FileType int

func (j JobStatus) IsFinished() bool {
	return j == Successed || j == Failed || j == Aborted
}
func (j JobStatus) IsFailed() bool {
	return j == Failed || j == Aborted
}
func (r JobStatus) String() string {
	switch r {
	case Created:
		return "Created"
	case Running:
		return "Running"
	case Successed:
		return "Successed"
	case Failed:
		return "Failed"
	case Aborted:
		return "Aborted"
	default:
		return "unknown"
	}
}
func (r *JobStatus) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return fmt.Errorf("data should be a string, got %s", data)
	}
	for _, status := range job_statuses {
		if status.String() == s {
			*r = status
			return nil
		}
	}
	return fmt.Errorf("invalid JobStatus %s", s)
}
func (r JobStatus) MarshalJSON() ([]byte, error) {
	return json.Marshal(r.String())
}
func (r JobStatus) GetDefaultExitCode() int {
	switch r {
	case Created:
		return 0
	case Running:
		return 0
	case Successed:
		return 0
	case Failed:
		return -1
	case Aborted:
		return -1
	default:
		return -123
	}
}

type JobResult struct {
	JobId    string
	Status   JobStatus
	Start    *time.Time
	End      *time.Time
	ExitCode int
	Message  string
}
type EventType int

const (
	WorkflowEvents EventType = iota
	JobEvents
	PipeEvents
)

type Event interface {
	GetEventType() EventType
}
type JobEvent struct {
	JobId     string
	Status    JobStatus
	Occured   time.Time
	execError error
	ExitCode  int
	Message   string
}

func (*JobEvent) GetEventType() EventType {
	return JobEvents
}

type Stream interface {
	Abort()
	Clear()
	Key() string
	Label() string
	UnBlock()
}
type Input interface {
	Stream
	GetWriter() (io.WriteCloser, error)
}
type Output interface {
	Stream
	GetReader() (io.ReadCloser, error)
	IsFailed() bool
}
type Job interface {
	GetId() string
	GetInputs() []Input
	GetOutputs() []Output
	Execute(wf *Workflow, wg *sync.WaitGroup)
	Abort()
	GetStatus() JobStatus
	GetResult() *JobResult
}
