package job

import (
	"fmt"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func GetEndPoint() string {
	mh := os.Getenv("MINIO_HOST")
	if mh != "" {
		return mh
	} else {
		return "miniotest:9000"
	}
}

func TestS3Upload(t *testing.T) {
	os1 := ObjectStore{
		AccessKey: "minioadminuser",
		SecretKey: "minioadminpassword",
		Endpoint:  GetEndPoint(),
		Region:    "ap-northeast-1",
	}
	err := os1.Init()
	if err != nil {
		t.Fatal("Fatal", err)
	}
	osi := ObjectStoreWriterCreator{
		Bucket:   "objectstoragetest",
		Key:      "s3_multipart_upload_test.dat",
		uploader: nil,
	}
	err = osi.Init()
	if err != nil {
		t.Fatal("Fatal", err)
	}
	writer, err := osi.CreateWriter()
	if err != nil {
		t.Fatal("Fatal", err)
	}
	r := strings.NewReader("object strage upload test")
	written, err := io.Copy(writer, r)
	if err != nil {
		t.Fatal("Fatal", err)
	}
	fmt.Printf("n=%d", written)
	err = writer.Close()
	if err != nil {
		t.Fatal("Fatal", err)
	}
}
func TestNotConnectError(t *testing.T) {
	endpoint := "dontexists:9000"

	os1 := ObjectStore{
		AccessKey: "minioadminuser",
		SecretKey: "minioadminpassword",
		Endpoint:  endpoint,
		Region:    "ap-northeast-1",
	}
	err := os1.Init()
	// TODO err must be reterned with not non-existent hostname
	assert.NoError(t, err)
}

func TestAccessKeyError(t *testing.T) {
	os1 := ObjectStore{
		AccessKey: "minioadminuser",
		SecretKey: "wrongpassword",
		Endpoint:  GetEndPoint(),
		Region:    "ap-northeast-1",
	}
	outputs := []WriterCreator{
		&PipeInfo{
			Name: "PIPE1",
			Path: "pipe1",
		},
	}
	os1.Init()
	handler := PipeHandler{
		Input: &ObjectStoreReaderCreator{
			Bucket: "objectstoragetest",
			Key:    "s3_multipart_upload_test.dat",
		},
		Outputs: outputs,
	}
	ch := make(chan Event)
	handler.Init()
	go handler.Handle(ch)
	pe := <-ch
	pe2 := pe.(*PipeEvent)
	assert.Equal(t, pe2.Status, Failed)
	assert.True(t, strings.HasPrefix(pe2.Message,
		"SignatureDoesNotMatch: The request signature we calculated does not match the signature you provided."))
}
func TestNotBucketExists(t *testing.T) {
	os1 := ObjectStore{
		AccessKey: "minioadminuser",
		SecretKey: "minioadminpassword",
		Endpoint:  GetEndPoint(),
		Region:    "ap-northeast-1",
	}
	outputs := []WriterCreator{
		&PipeInfo{
			Name: "PIPE1",
			Path: "pipe1",
		},
	}
	os1.Init()
	handler := PipeHandler{
		Input: &ObjectStoreReaderCreator{
			Bucket: "notexistsbucket",
			Key:    "s3_multipart_upload_test.dat",
		},
		Outputs: outputs,
	}
	ch := make(chan Event)
	handler.Init()
	go handler.Handle(ch)
	pe := <-ch
	pe2 := pe.(*PipeEvent)
	assert.Equal(t, pe2.Status, Failed)
	assert.True(t, strings.HasPrefix(pe2.Message,
		"NoSuchBucket: The specified bucket does not exist"))
}
