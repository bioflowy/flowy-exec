package job

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os/exec"
	"syscall"
)

type JobStatus int

const (
	Created JobStatus = iota
	Running
	Successed
	Failed
)

type FileType int

func (j JobStatus) IsFinished() bool {
	return j == Successed || j == Failed
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
	Command []string
	Inputs  []Input
	Outputs []Output
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
}
type JobEvent struct {
	job       *Job
	status    JobStatus
	execError error
	exitCode  int
	message   string
}

func (*JobEvent) GetEventType() EventType {
	return JobEvents
}

type WorkflowEvent struct {
	Status    JobStatus
	ExecError error
	ExitCode  int
	Message   string
}

func (we *WorkflowEvent) GetEventType() EventType {
	return WorkflowEvents
}

func (*PipeEvent) GetEventType() EventType {
	return PipeEvents
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
func LoadWorkflow(reader io.Reader) (*Workflow, error) {
	var workflow Workflow
	data, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(data, &workflow); err != nil {
		if jsonErr, ok := err.(*json.SyntaxError); ok {
			problemPart := data[jsonErr.Offset-10 : jsonErr.Offset+10]
			err = fmt.Errorf("%w ~ error near '%s' (offset %d)", err, problemPart, jsonErr.Offset)
		}
		fmt.Println(err)
		return nil, err
	}
	workflow.handlers = CreateHandlers(workflow.Jobs)
	return &workflow, nil
}
func (w *Workflow) GetStatus() JobStatus {
	status := Successed
	for _, job := range w.Jobs {
		if !job.status.IsFinished() {
			return Running
		}
		if job.status == Failed {
			status = Failed
		}
	}
	return status

}
func (w *Workflow) ExecuteWorkflow(status_ch chan Event) error {
	if w.Objectstore != nil {
		err := w.Objectstore.Init()
		if err != nil {
			return err
		}
	}
	job_event := make(chan Event, 1)
	for _, handler := range w.handlers {
		err := handler.Init()
		if err != nil {
			status_ch <- &PipeEvent{
				cause: err,
			}
		}
	}
	for _, job := range w.Jobs {
		go ExecuteJob(job, job_event)
	}
	for _, handler := range w.handlers {
		go handler.Handle(job_event)
	}
	for {
		ev := <-job_event
		switch ev.GetEventType() {
		case JobEvents:
			je := ev.(*JobEvent)
			if w.GetStatus().IsFinished() {
				print(je.status)
				status_ch <- &WorkflowEvent{
					Status: w.GetStatus(),
				}
				return nil
			}
		}
	}
}
func ExecuteJob(job *Job, ch chan Event) {
	cmd := exec.Command(job.Command[0], job.Command[1:]...)

	err := cmd.Start()
	if err != nil {
		job.status = Failed
		ch <- &JobEvent{
			job:       job,
			status:    Failed,
			execError: err,
			message:   err.Error(),
			exitCode:  -1,
		}
		return
	}
	job.status = Running
	err = cmd.Wait()
	if err != nil {
		if e2, ok := err.(*exec.ExitError); ok {
			if s, ok := e2.Sys().(syscall.WaitStatus); ok {
				job.status = Failed
				ch <- &JobEvent{
					job:       job,
					status:    Failed,
					execError: err,
					exitCode:  s.ExitStatus(),
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
			job:      job,
			status:   Successed,
			exitCode: 0,
		}
	}
}
