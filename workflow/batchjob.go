package workflow

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sync"
	"syscall"
	"time"

	"github.com/sirupsen/logrus"
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
	job     *BatchJob
	path    string
	key     string
	blocked bool
	handler *PipeHandler
}
type BatchJobInput struct {
	job     *BatchJob
	path    string
	key     string
	blocked bool
	handler *PipeHandler
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
	if s.job.status.IsFinished() {
		return nil, fmt.Errorf("Job %s has already finished", s.job.JobId)
	}
	logrus.WithFields(logrus.Fields{"jobId": s.job.JobId, "Path": s.path}).Info("Opening Writer")
	s.blocked = true
	w, err := os.OpenFile(s.path, os.O_WRONLY, 0)
	s.blocked = false
	if s.job.status.IsFinished() {
		return nil, fmt.Errorf("Job %s has already finished", s.job.JobId)
	}
	logrus.WithFields(logrus.Fields{"jobId": s.job.JobId, "Path": s.path}).Info("Opened Writer")
	return w, err
}
func (s *BatchJobOutput) GetReader() (io.ReadCloser, error) {
	if s.job.status.IsFinished() {
		return nil, fmt.Errorf("Job %s has already finished", s.job.JobId)
	}
	logrus.WithFields(logrus.Fields{"jobId": s.job.JobId, "Path": s.path}).Info("Opening Reader")
	s.blocked = true
	w, err := os.OpenFile(s.path, os.O_RDONLY, 0)
	s.blocked = false
	if s.job.status.IsFinished() {
		return nil, fmt.Errorf("Job %s has already finished", s.job.JobId)
	}
	logrus.WithFields(logrus.Fields{"jobId": s.job.JobId, "Path": s.path}).Info("Opened Reader")
	return w, err
}

func (s *BatchJobInput) UnBlock() {
	if s.blocked && s.job.status.IsFinished() {
		logrus.WithFields(logrus.Fields{"jobId": s.job.JobId, "Path": s.path}).Info("Unblock opening in write mode")
		r, err := os.OpenFile(s.path, os.O_RDONLY, 0)
		if err == nil {
			r.Close()
		}
	}
}
func (s *BatchJobOutput) UnBlock() {
	if s.blocked && s.job.status.IsFinished() {
		logrus.WithFields(logrus.Fields{"jobId": s.job.JobId, "Path": s.path}).Info("Unblock opening in read mode")
		r, err := os.OpenFile(s.path, os.O_WRONLY, 0)
		if err == nil {
			r.Close()
		}
	}
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
func (job *BatchJobOutput) Label() string {
	return job.job.JobId
}
func (job *BatchJobInput) Label() string {
	return job.job.JobId
}
func Exists(name string) bool {
	_, err := os.Stat(name)
	return !os.IsNotExist(err)
}
func (job *BatchJobOutput) Clear() {
	if Exists(job.path) {
		os.Remove(job.path)
	}
}
func (job *BatchJobInput) Clear() {
	if Exists(job.path) {
		os.Remove(job.path)
	}
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
		Start:    &job.Start,
		End:      &job.End,
		ExitCode: job.ExitCode,
		Message:  "",
	}
}

func (job *BatchJob) Execute(wf *Workflow, wg *sync.WaitGroup) {
	defer wg.Done()
	defer wf.UnBlock()
	logrus.WithFields(logrus.Fields{"jobId": job.JobId, "command": job.Command}).Info("Start Job")
	ctx := context.Background()
	ctx2, cancel := context.WithCancel(ctx)
	job._cancel = cancel
	cmd := exec.CommandContext(ctx2, job.Command[0], job.Command[1:]...)
	job.Start = time.Now()
	err := cmd.Start()
	if err != nil {
		job.status = Failed
		logrus.WithFields(logrus.Fields{"jobId": job.JobId, "status": job.status, "exitCode": job.ExitCode}).WithError(err).Warn("Finished Job")
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
					logrus.WithFields(logrus.Fields{"jobId": job.JobId, "status": job.status, "exitCode": job.ExitCode}).WithError(err).Warn("Job Aborted")
				} else {
					job.status = Failed
					job.ExitCode = s.ExitStatus()
					logrus.WithFields(logrus.Fields{"jobId": job.JobId, "status": job.status, "exitCode": job.ExitCode}).WithError(err).Warn("Job Failed")
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
		logrus.WithFields(logrus.Fields{"jobId": job.JobId, "status": job.status, "exitCode": job.ExitCode}).Info("Finished Job")
	}
}
