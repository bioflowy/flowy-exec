package workflow

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"sync"
	"time"
)

type WorkflowEvent struct {
	Status    JobStatus
	ExecError error
	ExitCode  int
	Message   string
}
type WorkflowDto struct {
	Objectstore *ObjectStore
	Jobs        []*JobDto
	handlers    []*PipeHandler
	Status      JobStatus
}
type Workflow struct {
	Objectstore *ObjectStore
	Jobs        []Job
	handlers    []*PipeHandler
	Status      JobStatus
}
type WorkflowResult struct {
	Status  JobStatus
	Results []*JobResult
	Start   *time.Time
	End     *time.Time
}
type JobDto struct {
	JobId    string
	Type     string
	Command  []string
	Inputs   []JobInput
	Outputs  []JobOutput
	Bucket   string
	Key      string
	WriteTo  string
	ReadFrom string
}
type JobInput struct {
	Path     string
	ReadFrom string
}

type JobOutput struct {
	Path    string
	WriteTo string
}

func LoadWorkflow(reader io.Reader) (*Workflow, error) {
	var workflow WorkflowDto
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
	jobIdMap := map[string]*JobDto{}
	for _, job := range workflow.Jobs {
		if _, ok := jobIdMap[job.JobId]; ok {
			err = fmt.Errorf("duplicated job id %s)", job.JobId)
			return nil, err
		}
		jobIdMap[job.JobId] = job
	}
	return CreateWorkflow(&workflow), nil
}
func CreateWorkflow(dto *WorkflowDto) *Workflow {
	jobs := make([]Job, 0, len(dto.Jobs))
	for _, jobDto := range dto.Jobs {
		var job Job
		switch jobDto.Type {
		case "ObjectStore":
			job = CreateObjectStoreJob(jobDto)
		default:
			job = CreateBatchJob(jobDto)
		}
		jobs = append(jobs, job)
	}
	return &Workflow{
		Objectstore: dto.Objectstore,
		Jobs:        jobs,
		Status:      Created,
	}
}

func CreateObjectStoreJob(jobDto *JobDto) Job {
	if jobDto.ReadFrom != "" {
		return &ObjectStoreUploadJob{
			jobId:    jobDto.JobId,
			status:   Created,
			readFrom: jobDto.ReadFrom,
			Bucket:   jobDto.Bucket,
			key:      jobDto.Key,
			closeCh:  make(chan JobStatus),
		}
	} else if jobDto.WriteTo != "" {
		return &ObjectStoreDownloadJob{
			jobId:   jobDto.JobId,
			status:  Created,
			writeTo: jobDto.WriteTo,
			Bucket:  jobDto.Bucket,
			key:     jobDto.Key,
			closeCh: make(chan JobStatus),
		}
	}
	panic("unimplemented")
}

func CreateBatchJob(jobDto *JobDto) Job {
	job := &BatchJob{
		JobId:   jobDto.JobId,
		status:  Created,
		Command: jobDto.Command,
		Inputs:  make([]BatchJobInput, len(jobDto.Inputs)),
		Outputs: make([]BatchJobOutput, len(jobDto.Outputs)),
	}
	for idx, input := range jobDto.Inputs {
		job.Inputs[idx] = BatchJobInput{
			job:  job,
			path: input.Path,
			key:  input.ReadFrom,
		}
	}
	for idx, output := range jobDto.Outputs {
		job.Outputs[idx] = BatchJobOutput{
			job:  job,
			path: output.Path,
			key:  output.WriteTo,
		}
	}
	return job
}
func (w *Workflow) GetStatus() JobStatus {
	status := Successed
	for _, job := range w.Jobs {
		if !job.GetStatus().IsFinished() {
			return Running
		}
		if job.GetStatus() == Failed || job.GetStatus() == Aborted {
			status = Failed
		}
	}
	return status
}
func (we *WorkflowEvent) GetEventType() EventType {
	return WorkflowEvents
}

func (w *Workflow) Execute(status_ch chan Event) *WorkflowResult {
	start := time.Now()
	if w.Objectstore != nil {
		err := w.Objectstore.Init()
		if err != nil {
			status_ch <- &WorkflowEvent{
				Status:    Failed,
				ExecError: err,
			}
			return &WorkflowResult{}
		}
	}
	handlers := CreateHandlers(w.Jobs)
	for _, handler := range handlers {
		handler.Init()
	}
	for _, handler := range handlers {
		go handler.Handle()
	}
	var wg sync.WaitGroup
	for _, job := range w.Jobs {
		wg.Add(1)
		go job.Execute(status_ch, &wg)
	}
	wg.Wait()
	for _, handler := range handlers {
		handler.Finished()
	}
	end := time.Now()
	status_ch <- &WorkflowEvent{
		Status:   w.GetStatus(),
		ExitCode: w.GetStatus().GetDefaultExitCode(),
	}
	results := make([]*JobResult, 0)
	for _, job := range w.Jobs {
		results = append(results, job.GetResult())
	}
	return &WorkflowResult{
		Status:  w.GetStatus(),
		Results: results,
		Start:   &start,
		End:     &end,
	}
}
