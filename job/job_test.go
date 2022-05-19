package job

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestExecuteJob(t *testing.T) {
	job := &Job{
		Command: []string{"notexists"},
	}
	status_ch := make(chan Event)
	go ExecuteJob(job, status_ch)
	e1 := <-status_ch
	assert.IsType(t, &JobEvent{}, e1, "A job Event must be dispatched")
	je := e1.(*JobEvent)
	assert.Equal(t, je.status, Failed, "Job status must be failed")
	assert.Equal(t, je.exitCode, -1, "Exit code must be -1")
	assert.Equal(t, je.message, "exec: \"notexists\": executable file not found in $PATH")
}
func TestExecuteJobSuccess(t *testing.T) {
	job := &Job{
		Command: []string{"pwd"},
	}
	status_ch := make(chan Event)
	go ExecuteJob(job, status_ch)
	e1 := <-status_ch
	assert.IsType(t, &JobEvent{}, e1, "A job Event must be dispatched")
	je := e1.(*JobEvent)
	assert.Equal(t, je.status, Successed, "Job status must be successed")
	assert.Equal(t, je.exitCode, 0, "Exit code must be 0")
	assert.Equal(t, je.message, "")
}
func TestExecuteJobFailed(t *testing.T) {
	job := &Job{
		Command: []string{"/bin/bash", "-c", "exit 123"},
	}
	status_ch := make(chan Event)
	go ExecuteJob(job, status_ch)
	e1 := <-status_ch
	assert.IsType(t, &JobEvent{}, e1, "A job Event must be dispatched")
	je := e1.(*JobEvent)
	assert.Equal(t, je.status, Failed, "Job status must be failed")
	assert.Equal(t, je.exitCode, 123, "Exit code must be 123")
	assert.Equal(t, je.message, "")
}
