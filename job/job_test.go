package job

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestExecuteJob(t *testing.T) {
	job := &Job{
		JobId:   "command1",
		Command: []string{"notexists"},
	}
	status_ch := make(chan Event)
	go ExecuteJob(job, status_ch)
	e1 := <-status_ch
	assert.IsType(t, &JobEvent{}, e1, "A job Event must be dispatched")
	je := e1.(*JobEvent)
	assert.Equal(t, je.Status, Failed, "Job status must be failed")
	assert.Equal(t, je.ExitCode, -1, "Exit code must be -1")
	assert.Equal(t, je.Message, "exec: \"notexists\": executable file not found in $PATH")
}
func TestExecuteJobSuccess(t *testing.T) {
	job := &Job{
		JobId:   "command1",
		Command: []string{"pwd"},
	}
	status_ch := make(chan Event)
	go ExecuteJob(job, status_ch)
	e1 := <-status_ch
	assert.IsType(t, &JobEvent{}, e1, "A job Event must be dispatched")
	je := e1.(*JobEvent)
	assert.Equal(t, je.Status, Successed, "Job status must be successed")
	assert.Equal(t, je.ExitCode, 0, "Exit code must be 0")
	assert.Equal(t, je.Message, "")
}
func TestExecuteJobFailed(t *testing.T) {
	job := &Job{
		JobId:   "command1",
		Command: []string{"/bin/bash", "-c", "exit 123"},
	}
	status_ch := make(chan Event)
	go ExecuteJob(job, status_ch)
	e1 := <-status_ch
	assert.IsType(t, &JobEvent{}, e1, "A job Event must be dispatched")
	je := e1.(*JobEvent)
	assert.Equal(t, je.JobId, "command1")
	assert.Equal(t, je.Status, Failed, "Job status must be failed")
	assert.Equal(t, je.ExitCode, 123, "Exit code must be 123")
	assert.Equal(t, je.Message, "")
}
func TestCancelJob(t *testing.T) {
	job := &Job{
		JobId:   "command1",
		Command: []string{"sleep", "100"},
	}
	status_ch := make(chan Event)
	go ExecuteJob(job, status_ch)
	go func() {
		time.Sleep(100 * time.Millisecond)
		job.Cancel()
	}()
	e1 := <-status_ch
	je := e1.(*JobEvent)
	assert.Equal(t, je.JobId, "command1")
	assert.Equal(t, je.Status, Aborted, "Job status must be aborted")
}
