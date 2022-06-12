package job

import (
	"bytes"
	"fmt"
	"io"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/endpoints"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

type ObjectStore struct {
	Region    string
	Endpoint  string
	AccessKey string
	SecretKey string
}
type ObjectStoreWriterCreator struct {
	Bucket   string
	Key      string
	uploader *ObjectStoreUploader
	job      *Job
}

type ObjectStoreUploader struct {
	output          *s3.CreateMultipartUploadOutput
	client          *s3.S3
	buff            []byte
	len             int64
	compuletedParts []*s3.CompletedPart
	partNumber      int64
	total_uploaded  int
	total_writed    int
}
type ObjectStoreReaderCreator struct {
	Path   string
	Bucket string
	Key    string
}

var session_1 *session.Session

func (p *ObjectStoreReaderCreator) CreateReader() (io.ReadCloser, error) {
	s3c := s3.New(session_1)
	out, err := s3c.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(p.Bucket),
		Key:    aws.String(p.Key),
	})
	return out.Body, err
}

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
func (p *ObjectStoreReaderCreator) Init() error {
	if session_1 == nil {
		config := &aws.Config{
			Region: aws.String(endpoints.ApNortheast1RegionID),
		}
		session_1, _ = session.NewSession(config)
	}
	return nil
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
			p.upload()
			curr = curr[len(buff):]
			buff = p.buff
		}
	}
}
func (p *ObjectStoreUploader) Close() error {
	err := p.upload()
	if err != nil {
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
	return err
}
func (p *ObjectStoreWriterCreator) Init() error {
	if session_1 != nil {
		client := s3.New(session_1)
		input := s3.CreateMultipartUploadInput{
			Bucket:      aws.String(p.Bucket),
			Key:         aws.String(p.Key),
			ContentType: aws.String("application/octet-stream"),
		}
		output, err := client.CreateMultipartUpload(&input)
		if err != nil {
			return err
		}
		uploader := ObjectStoreUploader{
			client:     client,
			output:     output,
			buff:       make([]byte, 10*1024*1024),
			len:        0,
			partNumber: 1,
		}
		p.uploader = &uploader
	}
	return nil
}
func (p *ObjectStoreWriterCreator) CreateWriter() (io.WriteCloser, error) {
	return p.uploader, nil
}
func (p *ObjectStoreWriterCreator) Abort() error {
	p.job.Cancel()
	return nil
}
