package workflow

import (
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/sirupsen/logrus"
)

type ObjectStore struct {
	Region    string
	Endpoint  string
	AccessKey string
	SecretKey string
}

var session_1 *session.Session

func (o *ObjectStore) Init() error {
	var c *credentials.Credentials
	if o.AccessKey != "" && o.SecretKey != "" {
		c = credentials.NewStaticCredentials(o.AccessKey, o.SecretKey, "")
	}
	var endpoints *string
	if o.Endpoint != "" {
		endpoints = &o.Endpoint
	}
	tmp := os.Getenv("OBJECTSTORE_ENDPOINT")
	if tmp != "" {
		endpoints = &tmp
	}
	config := &aws.Config{
		Region:           aws.String(o.Region),
		Credentials:      c,
		Endpoint:         endpoints,
		S3ForcePathStyle: aws.Bool(o.Endpoint != ""),
		DisableSSL:       aws.Bool(true),
	}
	var err error
	session_1, err = session.NewSession(config)
	return err
}

type ObjectStoreDownloadJob struct {
	jobId   string
	status  JobStatus
	writeTo string
	Bucket  string
	key     string
	Start   time.Time
	End     time.Time
	reader  io.ReadCloser
	closeCh chan JobStatus
}

func (job *ObjectStoreDownloadJob) GetResult() *JobResult {
	return &JobResult{
		JobId:    job.jobId,
		Status:   job.status,
		Start:    &job.Start,
		End:      &job.End,
		ExitCode: job.status.GetDefaultExitCode(),
		Message:  "",
	}
}

func (job *ObjectStoreDownloadJob) GetId() string {
	return job.jobId
}
func (job *ObjectStoreDownloadJob) GetStatus() JobStatus {
	return job.status
}

func (job *ObjectStoreDownloadJob) GetInputs() []Input {
	return []Input{}
}

func (job *ObjectStoreDownloadJob) GetOutputs() []Output {
	return []Output{job}
}
func (job *ObjectStoreDownloadJob) Label() string {
	return job.key
}

func (job *ObjectStoreDownloadJob) IsFailed() bool {
	return job.status.IsFailed()
}
func (job *ObjectStoreDownloadJob) Clear() {
}

func (job *ObjectStoreDownloadJob) Execute(wf *Workflow, wg *sync.WaitGroup) {
	defer wg.Done()
	defer wf.UnBlock()
	if job.status.IsFinished() {
		logrus.WithFields(logrus.Fields{"jobId": job.jobId, "status": job.status, "exitCode": -1}).Warn("Job Failed")
		return
	}
	job.status = Running
	status := <-job.closeCh
	if status.IsFinished() {
		job.status = status
		if status == Successed {
			logrus.WithFields(logrus.Fields{"jobId": job.jobId, "status": job.status, "exitCode": 0}).Warn("Job Finished")
		} else {
			logrus.WithFields(logrus.Fields{"jobId": job.jobId, "status": job.status, "exitCode": -1}).Warn("Job Failed")
		}
	}
}

func (job *ObjectStoreDownloadJob) Abort() {
	job.closeCh <- Aborted
}

func (job *ObjectStoreDownloadJob) Read(p []byte) (n int, err error) {
	return job.reader.Read(p)
}

func (job *ObjectStoreDownloadJob) Close() error {
	err := job.reader.Close()
	job.closeCh <- Successed
	return err
}

func (job *ObjectStoreDownloadJob) Key() string {
	return job.writeTo
}
func (p *ObjectStoreDownloadJob) UnBlock() {

}

func (job *ObjectStoreDownloadJob) GetReader() (io.ReadCloser, error) {
	if session_1 == nil {
		return nil, fmt.Errorf("s3 session is not initialized")
	}
	s3c := s3.New(session_1)
	out, err := s3c.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(job.Bucket),
		Key:    aws.String(job.key),
	})
	if err != nil {
		job.status = Failed
		return nil, err
	} else {
		job.reader = out.Body
		return job, nil
	}
}
