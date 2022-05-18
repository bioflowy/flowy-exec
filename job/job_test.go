package job

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestExecuteJob(t *testing.T) {
	job := &Job{
		Command: []string{"notexists"},
	}
	status_ch := make(chan *JobEvent)
	go ExecuteJob(job, status_ch)
	e1 := <-status_ch
	assert.Equal(t, e1.status, Failed, "Job status must be failed")
	assert.Equal(t, e1.exitCode, -1, "Exit code must be -1")
	assert.Equal(t, e1.message, "exec: \"notexists\": executable file not found in $PATH")
}
func TestExecuteJobSuccess(t *testing.T) {
	job := &Job{
		Command: []string{"pwd"},
	}
	status_ch := make(chan *JobEvent)
	go ExecuteJob(job, status_ch)
	e1 := <-status_ch
	assert.Equal(t, e1.status, Successed, "Job status must be successed")
	assert.Equal(t, e1.exitCode, 0, "Exit code must be 0")
	assert.Equal(t, e1.message, "")
}
func TestExecuteJobFailed(t *testing.T) {
	job := &Job{
		Command: []string{"/bin/bash", "-c", "exit 123"},
	}
	status_ch := make(chan *JobEvent)
	go ExecuteJob(job, status_ch)
	e1 := <-status_ch
	assert.Equal(t, e1.status, Failed, "Job status must be failed")
	assert.Equal(t, e1.exitCode, 123, "Exit code must be 123")
	assert.Equal(t, e1.message, "")
}
