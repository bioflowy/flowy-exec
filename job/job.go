package job

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"syscall"
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

type FileType int

func (j JobStatus) IsFinished() bool {
	return j == Successed || j == Failed || j == Aborted
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

const (
	File FileType = iota
	Pipe
	ObjectStrage
)

type EventType int

const (
	WorkflowEvents EventType = iota
	JobEvents
	PipeEvents
)

func (r *FileType) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return fmt.Errorf("data should be a string, got %s", data)
	}

	var ur FileType
	switch s {
	case "File":
		ur = File
	case "FIFO":
		ur = Pipe
	case "ObjectStore":
		ur = ObjectStrage
	default:
		return fmt.Errorf("invalid FileType %s", s)

	}
	*r = ur
	return nil
}

func (r *JobStatus) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return fmt.Errorf("data should be a string, got %s", data)
	}

	switch s {
	case Created.String():
		*r = Created
	case Running.String():
		*r = Running
	case Successed.String():
		*r = Successed
	case Failed.String():
		*r = Failed
	case Aborted.String():
		*r = Aborted
	default:
		return fmt.Errorf("invalid JobStatus %s", s)
	}
	return nil
}
func (r JobStatus) MarshalJSON() ([]byte, error) {
	return json.Marshal(r.String())
}

type Input struct {
	Type   FileType
	Path   string
	Name   string
	Bucket string
	Key    string
}

type Output struct {
	Type   FileType
	Path   string
	Name   string
	Bucket string
	Key    string
}
type Job struct {
	JobId   string
	Command []string
	Inputs  []Input
	Outputs []Output
	_cancel context.CancelFunc
	status  JobStatus
}
type Workflow struct {
	Objectstore *ObjectStore
	Jobs        []*Job
	handlers    []*PipeHandler
	Status      JobStatus
}
type Event interface {
	GetEventType() EventType
	ToJson() (string, error)
}
type JobEvent struct {
	JobId     string
	Status    JobStatus
	Occured   time.Time
	execError error
	ExitCode  int
	Message   string
}
type EventJson struct {
	JobName string
	Status  string
	occured time.Time
	message string
}

func (*JobEvent) GetEventType() EventType {
	return JobEvents
}
func (j *JobEvent) ToJson() (string, error) {
	b, err := json.Marshal(j)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func (*PipeEvent) GetEventType() EventType {
	return PipeEvents
}

func (j *PipeEvent) ToJson() (string, error) {
	b, err := json.Marshal(j)
	if err != nil {
		return "", err
	}
	return string(b), nil
}
func (j *WorkflowEvent) ToJson() (string, error) {
	b, err := json.Marshal(j)
	if err != nil {
		return "", err
	}
	return string(b), nil
}
func LoadFromFile(data []byte) ([]*Job, error) {
	var jobs []*Job
	if err := json.Unmarshal(data, &jobs); err != nil {
		if jsonErr, ok := err.(*json.SyntaxError); ok {
			problemPart := data[jsonErr.Offset-10 : jsonErr.Offset+10]
			err = fmt.Errorf("%w ~ error near '%s' (offset %d)", err, problemPart, jsonErr.Offset)
		}
		fmt.Println(err)
		return nil, err
	}
	return jobs, nil
}
func (j *Job) Cancel() {
	j._cancel()
	j.status = Aborted
}
func ExecuteJob(job *Job, ch chan Event) {
	ctx := context.Background()
	ctx2, cancel := context.WithCancel(ctx)
	job._cancel = cancel
	cmd := exec.CommandContext(ctx2, job.Command[0], job.Command[1:]...)
	err := cmd.Start()
	if err != nil {
		job.status = Failed
		ch <- &JobEvent{
			JobId:     job.JobId,
			Status:    Failed,
			execError: err,
			Message:   err.Error(),
			ExitCode:  -1,
		}
		return
	}
	job.status = Running
	err = cmd.Wait()
	if err != nil {
		if e2, ok := err.(*exec.ExitError); ok {
			if s, ok := e2.Sys().(syscall.WaitStatus); ok {
				if job.status == Aborted {
					ch <- &JobEvent{
						JobId:     job.JobId,
						Status:    Aborted,
						execError: err,
						ExitCode:  s.ExitStatus(),
					}

				} else {
					job.status = Failed
					ch <- &JobEvent{
						JobId:     job.JobId,
						Status:    Failed,
						execError: err,
						ExitCode:  s.ExitStatus(),
					}
				}
			} else {
				panic(errors.New("Unimplemented for system where exec.ExitError.Sys() is not syscall.WaitStatus."))
			}
		} else {
			panic(errors.New("Unimplemented for system where exec.ExitError.Sys() is not syscall.WaitStatus."))
		}
	} else {
		job.status = Successed
		ch <- &JobEvent{
			JobId:    job.JobId,
			Status:   Successed,
			ExitCode: 0,
		}
	}
}
