package s3

import (
	"context"
	"fmt"
	"io"
	"log"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/hatchify/errors"
)

const (
	// ErrInvalidName is returned when a name is empty
	ErrInvalidName = errors.Error("invalid name, cannot be empty")
	// ErrInvalidDirectory is returned when a directory is empty
	ErrInvalidDirectory = errors.Error("invalid directory, cannot be empty")
)

func New(o Options) (sp *S3, err error) {
	// Load the SDK's configuration from environment and shared config, and
	// create the client with this.
	cfg, err := config.LoadDefaultConfig(context.Background(), config.WithRegion(o.Region))
	if err != nil {
		log.Fatalf("failed to load SDK configuration, %v", err)
	}

	var s S3
	s.o = o
	s.s3 = s3.NewFromConfig(cfg)

	if err = s.createBucket(context.Background()); err != nil {
		return
	}

	sp = &s
	return
}

type S3 struct {
	o  Options
	s3 *s3.Client
}

func (s *S3) Export(ctx context.Context, filename string, r io.Reader) (err error) {
	input := &s3.PutObjectInput{
		Bucket:               aws.String(s.o.Bucket),
		Key:                  aws.String(filename),
		Body:                 r,
		ServerSideEncryption: "AES256",
		ACL:                  "private",
	}

	_, err = s.s3.PutObject(ctx, input)
	return
}

func (s *S3) Import(ctx context.Context, filename string, w io.Writer) (err error) {
	getInput := s3.GetObjectInput{
		Bucket: aws.String(s.o.Bucket),
		Key:    aws.String(filename),
	}

	var out *s3.GetObjectOutput
	if out, err = s.s3.GetObject(ctx, &getInput); err != nil {
		return
	}
	defer out.Body.Close()

	_, err = io.Copy(w, out.Body)
	return
}

func (s *S3) GetNext(ctx context.Context, prefix, lastFilename string) (nextKey string, err error) {
	input := s3.ListObjectsV2Input{
		Bucket:     aws.String(s.o.Bucket),
		Prefix:     aws.String(prefix),
		StartAfter: aws.String(lastFilename),
		MaxKeys:    1,
	}

	var out *s3.ListObjectsV2Output
	if out, err = s.s3.ListObjectsV2(ctx, &input); err != nil {
		return
	}

	if len(out.Contents) == 0 {
		err = io.EOF
		return
	}

	nextKey = *out.Contents[0].Key
	return
}

func (s *S3) createBucket(ctx context.Context) (err error) {
	opts := &s3.CreateBucketInput{
		Bucket: aws.String(s.o.Bucket),
		ACL:    "private",
	}

	_, err = s.s3.CreateBucket(ctx, opts)
	switch {
	case err == nil:
	//case strings.Contains(err.Error(), s3.ErrCodeBucketAlreadyExists):
	//case strings.Contains(err.Error(), s3.ErrCodeBucketAlreadyOwnedByYou):

	default:
		fmt.Printf("THIS ONE <%v>\n", err)
		return
	}

	return nil
}

func (s *S3) deleteBucket(ctx context.Context) (err error) {
	if err = s.emptyBucket(ctx); err != nil {
		return
	}

	input := &s3.DeleteBucketInput{
		Bucket: aws.String(s.o.Bucket),
	}

	_, err = s.s3.DeleteBucket(ctx, input)
	return
}

func (s *S3) emptyBucket(ctx context.Context) (err error) {
	input := s3.DeleteObjectsInput{
		Bucket: &s.o.Bucket,
	}

	_, err = s.s3.DeleteObjects(ctx, &input)
	return
}
