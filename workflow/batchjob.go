package workflow

import (
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"sync"
	"syscall"
	"time"
)

type BatchJob struct {
	JobId    string
	Command  []string
	Inputs   []BatchJobInput
	Outputs  []BatchJobOutput
	_cancel  context.CancelFunc
	status   JobStatus
	ExitCode int
	Start    time.Time
	End      time.Time
}
type BatchJobOutput struct {
	job  *BatchJob
	path string
	key  string
}
type BatchJobInput struct {
	job  *BatchJob
	path string
	key  string
}

func (s *BatchJobOutput) Abort() {
	s.job.Abort()
}

func (s *BatchJobOutput) Key() string {
	return s.key
}
func (s *BatchJobInput) Abort() {
	s.job.Abort()
}

func (s *BatchJobInput) Key() string {
	return s.key
}

func (s *BatchJobInput) GetWriter() (io.WriteCloser, error) {
	return os.OpenFile(s.path, os.O_WRONLY, 0)
}
func (s *BatchJobOutput) GetReader() (io.ReadCloser, error) {
	return os.OpenFile(s.path, os.O_RDONLY, 0)
}

func (job *BatchJob) GetId() string {
	return job.JobId
}

func (job *BatchJob) GetInputs() []Input {
	inputs := make([]Input, 0, len(job.Inputs))
	for _, input := range job.Inputs {
		inputs = append(inputs, &BatchJobInput{
			job:  job,
			key:  input.key,
			path: input.path,
		})
		syscall.Mkfifo(input.path, 0600)
	}
	return inputs
}

func (job *BatchJob) GetOutputs() []Output {
	outputs := make([]Output, 0, len(job.Outputs))
	for _, output := range job.Outputs {
		outputs = append(outputs, &BatchJobOutput{
			job:  job,
			key:  output.key,
			path: output.path,
		})
		syscall.Mkfifo(output.path, 0600)
	}
	return outputs
}
func (job *BatchJobOutput) IsFailed() bool {
	return job.job.status.IsFailed()
}
func (job *BatchJob) GetStatus() JobStatus {
	return job.status
}
func (job *BatchJob) Abort() {
	job._cancel()
	job.status = Aborted
}

func (job *BatchJob) GetResult() *JobResult {
	return &JobResult{
		JobId:    job.JobId,
		Status:   job.status,
		Start:    job.Start,
		End:      job.End,
		ExitCode: job.ExitCode,
		Message:  "",
	}
}

func (job *BatchJob) Execute(ch chan Event, wg *sync.WaitGroup) {
	defer wg.Done()
	ctx := context.Background()
	ctx2, cancel := context.WithCancel(ctx)
	job._cancel = cancel
	cmd := exec.CommandContext(ctx2, job.Command[0], job.Command[1:]...)
	job.Start = time.Now()
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
	job.End = time.Now()
	if err != nil {
		if e2, ok := err.(*exec.ExitError); ok {
			if s, ok := e2.Sys().(syscall.WaitStatus); ok {
				if job.status == Aborted {
					job.status = Aborted
					job.ExitCode = Aborted.GetDefaultExitCode()
					ch <- &JobEvent{
						JobId:     job.JobId,
						Status:    Aborted,
						execError: err,
						ExitCode:  job.ExitCode,
					}

				} else {
					job.status = Failed
					job.ExitCode = s.ExitStatus()
					ch <- &JobEvent{
						JobId:     job.JobId,
						Status:    Failed,
						execError: err,
						ExitCode:  s.ExitStatus(),
					}
				}
			} else {
				panic(errors.New("unimplemented for system where exec.ExitError.Sys() is not syscall.WaitStatus"))
			}
		} else {
			panic(errors.New("unimplemented for system where exec.ExitError.Sys() is not syscall.WaitStatus"))
		}
	} else {
		job.status = Successed
		job.ExitCode = 0
		ch <- &JobEvent{
			JobId:    job.JobId,
			Status:   Successed,
			ExitCode: 0,
		}
	}
}
