package job

import (
	"encoding/json"
	"errors"
	"fmt"
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
	case "S3":
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
	jobs     []*Job
	handlers []*PipeHandler
	status   JobStatus
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
	status    JobStatus
	execError error
	exitCode  int
	message   string
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
func LoadWorkflow(data []byte) (*Workflow, error) {
	var jobs []*Job
	if err := json.Unmarshal(data, &jobs); err != nil {
		if jsonErr, ok := err.(*json.SyntaxError); ok {
			problemPart := data[jsonErr.Offset-10 : jsonErr.Offset+10]
			err = fmt.Errorf("%w ~ error near '%s' (offset %d)", err, problemPart, jsonErr.Offset)
		}
		fmt.Println(err)
		return nil, err
	}
	handlers := CreateHandlers(jobs)
	Workflow := &Workflow{
		jobs:     jobs,
		handlers: handlers,
		status:   Created,
	}
	return Workflow, nil
}
func (w *Workflow) ExecuteWorkflow(status_ch chan *JobEvent) error {
	ch := make(chan error, 1)
	chs := make([]chan *PipeEvent, len(w.handlers))
	for _, handler := range w.handlers {
		handler.Init()
	}
	for _, job := range w.jobs {
		go ExecuteJob(job, status_ch)
	}
	for i, handler := range w.handlers {
		chs[i] = make(chan *PipeEvent, 1)
		go handler.Handle(chs[i])
	}
	err := <-ch
	if err != nil {
		return err
	}
	for _, ch2 := range chs {
		pe := <-ch2
		if pe.cause != nil {
			return pe.cause
		}

	}
	return nil
}
func ExecuteJob(job *Job, ch chan *JobEvent) {
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
		ch <- &JobEvent{
			job:      job,
			status:   Successed,
			exitCode: 0,
		}
	}
}
