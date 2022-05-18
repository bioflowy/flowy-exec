package job

import (
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
)

func TestS3Upload(t *testing.T) {
	endpoint := "miniotest:9000"
	mh := os.Getenv("MINIO_HOST")
	if mh != "" {
		endpoint = mh + ":9000"
	}
	os1 := ObjectStore{
		AccessKey: "minioadminuser",
		SecretKey: "minioadminpassword",
		Endpoint:  endpoint,
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
