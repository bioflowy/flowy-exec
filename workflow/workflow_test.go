package workflow

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func testWorkflow(t *testing.T, path string, expected *WorkflowResult) {
	j, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	workflow, err := LoadWorkflow(j)
	if err != nil {
		t.Fatal(err)
	}
	ch := make(chan Event, 10)
	ev := workflow.Execute(ch)
	assertWorkflowResult(t, expected, ev)
}
func TestS3DownloadWorkflow(t *testing.T) {
	evs := WorkflowResult{
		Status: Successed,
		results: []*JobResult{
			{
				JobId:    "download",
				Status:   Successed,
				ExitCode: 0,
			},
			{
				JobId:    "wordcount",
				Status:   Successed,
				ExitCode: 0,
			},
		},
	}

	testWorkflow(t, "../testdata/s3_download.json", &evs)
}

func TestS3UploadWorkflow(t *testing.T) {
	evs := WorkflowResult{
		Status: Successed,
		results: []*JobResult{
			{
				JobId:    "ls-l",
				Status:   Successed,
				ExitCode: 0,
			},
			{
				JobId:    "uploadS3",
				Status:   Successed,
				ExitCode: 0,
			},
		},
	}
	testWorkflow(t, "../testdata/s3_upload.json", &evs)
}

func TestS3DownloadNoBucket(t *testing.T) {
	evs := WorkflowResult{
		Status: Failed,
		results: []*JobResult{
			{
				JobId:    "download",
				Status:   Aborted,
				ExitCode: -1,
			},
			{
				JobId:    "wordcount",
				Status:   Aborted,
				ExitCode: -1,
			},
		},
	}
	testWorkflow(t, "../testdata/s3_download_notexists_bucket.json", &evs)
}

func TestS3UploadNoBucket(t *testing.T) {
	evs := WorkflowResult{
		Status: Failed,
		results: []*JobResult{
			{
				JobId:    "ls-l",
				Status:   Failed,
				ExitCode: -1,
			},
			{
				JobId:    "uploadS3",
				Status:   Failed,
				ExitCode: -1,
			},
		},
	}
	testWorkflow(t, "../testdata/s3_upload_bucket_notexists.json", &evs)
}

func TestS3UploadCmdErr(t *testing.T) {
	evs := WorkflowResult{
		Status: Failed,
		results: []*JobResult{

			{
				JobId:    "testcmd",
				Status:   Failed,
				ExitCode: 222,
			},
			{
				JobId:    "uploadS3",
				Status:   Aborted,
				ExitCode: -1,
			},
		},
	}
	testWorkflow(t, "../testdata/s3_upload_cmd_err.json", &evs)
}

func assertJobEvent(t *testing.T, expected Event, actual Event) {
	assert.IsType(t, expected, actual, "Must be JobEvent")
	expectedJE := expected.(*JobEvent)
	actualJE := actual.(*JobEvent)
	assert.Equal(t, expectedJE.JobId, actualJE.JobId)
	assert.Equal(t, expectedJE.ExitCode, actualJE.ExitCode)
	assert.Equal(t, expectedJE.Status.String(), actualJE.Status.String())
}
func assertWorkflowEvent(t *testing.T, expected Event, actual Event) {
	assert.IsType(t, expected, actual, "Must be WorkflowEvent")
	expectedJE := expected.(*WorkflowEvent)
	actualJE := actual.(*WorkflowEvent)
	assert.Equal(t, expectedJE.ExitCode, actualJE.ExitCode)
}
func assertEvent(t *testing.T, expected Event, actual Event) {
	switch expected.GetEventType() {
	case JobEvents:
		assertJobEvent(t, expected, actual)
	case WorkflowEvents:
		assertWorkflowEvent(t, expected, actual)
	}
}
func assertJobResult(t *testing.T, expected *JobResult, actual *JobResult) {
	assert.Equal(t, expected.JobId, actual.JobId)
	assert.Equal(t, expected.Status.String(), actual.Status.String())
	assert.Equal(t, expected.ExitCode, actual.ExitCode)
}
func assertWorkflowResult(t *testing.T, expected *WorkflowResult, actual *WorkflowResult) {
	assert.Equal(t, expected.Status.String(), actual.Status.String())
	for idx, jr := range actual.results {
		assertJobResult(t, expected.results[idx], jr)
	}
}
