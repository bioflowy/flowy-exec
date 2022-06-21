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
		Results: []*JobResult{
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
		Results: []*JobResult{
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
		Results: []*JobResult{
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
		Results: []*JobResult{
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
		Results: []*JobResult{

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
func TestS3UploadCmdErr2(t *testing.T) {
	evs := WorkflowResult{
		Status: Failed,
		Results: []*JobResult{

			{
				JobId:    "testcmd",
				Status:   Failed,
				ExitCode: 222,
			},
			{
				JobId:    "wc",
				Status:   Aborted,
				ExitCode: -1,
			},
		},
	}
	testWorkflow(t, "../testdata/s3_upload_cmd_err2.json", &evs)
}
func TestForkPipe(t *testing.T) {
	evs := WorkflowResult{
		Status: Successed,
		Results: []*JobResult{

			{
				JobId:    "testcmd",
				Status:   Successed,
				ExitCode: 0,
			},
			{
				JobId:    "wordcount",
				Status:   Successed,
				ExitCode: 0,
			},
			{
				JobId:    "grep",
				Status:   Successed,
				ExitCode: 0,
			},
		},
	}
	testWorkflow(t, "../testdata/forked_pipe.json", &evs)
}
func TestForkPipeError(t *testing.T) {
	evs := WorkflowResult{
		Status: Failed,
		Results: []*JobResult{

			{
				JobId:    "testcmd",
				Status:   Successed,
				ExitCode: 0,
			},
			{
				JobId:    "wordcount",
				Status:   Successed,
				ExitCode: 0,
			},
			{
				JobId:    "error",
				Status:   Failed,
				ExitCode: 123,
			},
		},
	}
	testWorkflow(t, "../testdata/forked_pipe_error.json", &evs)
}

func assertJobResult(t *testing.T, expected *JobResult, actual *JobResult) {
	assert.Equal(t, expected.JobId, actual.JobId)
	assert.Equal(t, expected.Status.String(), actual.Status.String())
	assert.Equal(t, expected.ExitCode, actual.ExitCode)
}
func assertWorkflowResult(t *testing.T, expected *WorkflowResult, actual *WorkflowResult) {
	assert.Equal(t, expected.Status.String(), actual.Status.String())
	for idx, jr := range actual.Results {
		assertJobResult(t, expected.Results[idx], jr)
	}
}
