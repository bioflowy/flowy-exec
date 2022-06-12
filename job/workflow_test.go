package job

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLoadWorkflowDuplicateId(t *testing.T) {
	j, err := os.Open("../testdata/workflow_dup_job_id.json")
	if err != nil {
		t.Fatal(err)
	}
	_, err = LoadWorkflow(j)
	assert.Error(t, err)
}
func assertEvent(t *testing.T,expected Event,actual Event,){
	switch(actual.GetEventType()){
	case PipeEvents:
		assert.IsType(t,expected,actual)
	case JobEvents:
		assert.IsType(t,expected,actual)
	case WorkflowEvents:
		assert.IsType(t,expected,actual)
	}
}
func TestS3DownloadBucketNotExists(t *testing.T) {
	j, err := os.Open("../testdata/s3_download_notexists_bucket.json")
	if err != nil {
		t.Fatal(err)
	}
	wf, err := LoadWorkflow(j)
	ch := make(chan Event)
	go wf.ExecuteWorkflow(ch)
	events := []Event{
		&PipeEvent{
			Status: Failed,
			Message: "NoSuchBucket: The specified bucket does not exist",
		},
		&JobEvent{
			Status: Aborted,
			ExitCode: -1,
			Message: "",
		},
		&WorkflowEvent{
			Status: Failed,
			ExitCode: -1,
			Message: "",
		},
	}
	for _,expected := range events{
		ev := <-ch
		assertEvent(t,expected,ev)
	}

}
func TestS3UploadBucketNotExists(t *testing.T) {
	j, err := os.Open("../testdata/s3_upload_bucket_notexists.json")
	if err != nil {
		t.Fatal(err)
	}
	wf, err := LoadWorkflow(j)
	ch := make(chan Event)
	go wf.ExecuteWorkflow(ch)
	events := []Event{
		&PipeEvent{
			Status: Failed,
			Message: "NoSuchBucket: The specified bucket does not exist",
		},
		&WorkflowEvent{
			Status: Failed,
			ExitCode: -1,
			Message: "",
		},
	}
	for _,expected := range events{
		ev := <-ch
		assertEvent(t,expected,ev)
	}

}
