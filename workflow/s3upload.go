package workflow

import (
	"bytes"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

type ObjectStoreUploadJob struct {
	jobId    string
	status   JobStatus
	readFrom string
	Bucket   string
	key      string
	uploader *ObjectStoreUploader
	closeCh  chan JobStatus
	Start    time.Time
	End      time.Time
}

type ObjectStoreUploader struct {
	job             *ObjectStoreUploadJob
	output          *s3.CreateMultipartUploadOutput
	client          *s3.S3
	buff            []byte
	len             int64
	compuletedParts []*s3.CompletedPart
	partNumber      int64
	total_uploaded  int
	total_writed    int
}

func (p *ObjectStoreUploader) upload() error {
	if p.len <= 0 {
		return nil
	}
	PartNumber := aws.Int64(p.partNumber)
	input := s3.UploadPartInput{
		Body:          bytes.NewReader(p.buff[:p.len]),
		Bucket:        p.output.Bucket,
		Key:           p.output.Key,
		PartNumber:    PartNumber,
		UploadId:      p.output.UploadId,
		ContentLength: aws.Int64(p.len),
	}
	resp, err := p.client.UploadPart(&input)
	if err != nil {
		p.job.status = Failed
		return err
	}
	completedPart := s3.CompletedPart{
		ETag:       resp.ETag,
		PartNumber: PartNumber,
	}
	fmt.Printf("uploaded %d bytes\n", p.len)
	p.total_uploaded += int(p.len)
	p.compuletedParts = append(p.compuletedParts, &completedPart)
	p.len = 0
	p.partNumber++
	return err
}
func (p *ObjectStoreUploader) Write(data []byte) (n int, err error) {
	p.total_writed += len(data)
	curr := data
	buff := p.buff[p.len:]
	var total_writed int = 0
	for {
		if len(curr) < len(buff) {
			copy(buff, curr)
			p.len += int64(len(curr))
			// copy to buffer and return if buffer is not full
			total_writed += len(curr)
			return total_writed, nil
		} else {
			copy(buff, curr[:len(buff)])
			p.len += int64(len(curr))
			total_writed += len(buff)
			err := p.upload()
			if err != nil {
				p.job.status = Failed
				return total_writed, err
			}
			curr = curr[len(buff):]
			buff = p.buff
		}
	}
}
func (p *ObjectStoreUploader) Abort() {
	p.job.Abort()
}
func (p *ObjectStoreUploader) Key() string {
	return p.job.key
}
func (p *ObjectStoreUploader) Close() error {
	err := p.upload()
	if err != nil {
		p.job.status = Failed
		p.job.closeCh <- Failed
		return err
	}
	resp := p.output
	completeInput := s3.CompleteMultipartUploadInput{
		Bucket:   resp.Bucket,
		Key:      resp.Key,
		UploadId: resp.UploadId,
		MultipartUpload: &s3.CompletedMultipartUpload{
			Parts: p.compuletedParts,
		},
	}
	_, err = p.client.CompleteMultipartUpload(&completeInput)
	if err != nil {
		p.job.status = Failed
	} else {
		p.job.status = Successed
	}
	p.job.closeCh <- p.job.status
	return err
}
func (p *ObjectStoreUploadJob) Execute(ch chan Event, wg *sync.WaitGroup) {
	defer wg.Done()
	p.status = Running
	p.Start = time.Now()
	status := <-p.closeCh
	p.closeCh = nil

	if status.IsFinished() {
		p.End = time.Now()
		p.status = status
		ch <- &JobEvent{
			JobId:    p.jobId,
			Status:   status,
			ExitCode: status.GetDefaultExitCode(),
		}
	}
}
func (job *ObjectStoreUploadJob) GetResult() *JobResult {
	return &JobResult{
		JobId:    job.jobId,
		Status:   job.status,
		Start:    job.Start,
		End:      job.End,
		ExitCode: job.status.GetDefaultExitCode(),
		Message:  "",
	}
}
func (p *ObjectStoreUploadJob) GetId() string {
	return p.jobId
}
func (p *ObjectStoreUploadJob) GetStatus() JobStatus {
	return p.status
}
func (p *ObjectStoreUploadJob) Abort() {
	p.status = Aborted
	if p.closeCh != nil {
		p.closeCh <- Aborted
	}
}
func (p *ObjectStoreUploadJob) Key() string {
	return p.readFrom
}

func (p *ObjectStoreUploadJob) GetInputs() []Input {
	return []Input{p}
}

func (p *ObjectStoreUploadJob) GetOutputs() []Output {
	return []Output{}
}

func (p *ObjectStoreUploadJob) GetWriter() (io.WriteCloser, error) {
	if session_1 == nil {
		return nil, fmt.Errorf("s3 session is not initialized")
	}
	client := s3.New(session_1)
	input := s3.CreateMultipartUploadInput{
		Bucket:      aws.String(p.Bucket),
		Key:         aws.String(p.key),
		ContentType: aws.String("application/octet-stream"),
	}
	output, err := client.CreateMultipartUpload(&input)
	if err != nil {
		return nil, err
	}
	uploader := ObjectStoreUploader{
		job:        p,
		client:     client,
		output:     output,
		buff:       make([]byte, 10*1024*1024),
		len:        0,
		partNumber: 1,
	}
	p.uploader = &uploader
	return &uploader, nil
}
