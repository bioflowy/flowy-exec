package job

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
)

type WorkflowEvent struct {
	Status    JobStatus
	ExecError error
	ExitCode  int
	Message   string
}

func (we *WorkflowEvent) GetEventType() EventType {
	return WorkflowEvents
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
	jobIdMap := map[string]*Job{}
	for _, job := range workflow.Jobs {
		if _, ok := jobIdMap[job.JobId]; ok {
			err = fmt.Errorf("duplicated job id %s)", job.JobId)
			return nil, err
		}
		jobIdMap[job.JobId] = job
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
	for _, handler := range w.handlers {
		if !handler.Status.IsFinished() {
			return Running
		}
		if handler.Status == Failed {
			status = Failed
		}
	}
	return status
}
func (w *Workflow) ExecuteWorkflow(status_ch chan Event) {
	if w.Objectstore != nil {
		err := w.Objectstore.Init()
		if err != nil {
			status_ch <- &WorkflowEvent{
				Status: Failed,
				ExecError: err,
			}
			return
		}
	}
	job_event := make(chan Event, 1)
	for _, handler := range w.handlers {
		err := handler.Init()
		if err != nil {
			status_ch <- &PipeEvent{
				Status: Failed,
				cause: err,
			}
			status_ch <- &WorkflowEvent{
				Status: Failed,
				ExecError: err,
			}
			w.Status = Failed
			return
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
		status_ch <- ev
		switch ev.GetEventType() {
		case JobEvents:
			if w.GetStatus().IsFinished() {
				status_ch <- &WorkflowEvent{
					Status: w.GetStatus(),
				}
				return
			}
		}
	}
}
