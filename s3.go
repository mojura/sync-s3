package s3

import (
	"context"
	"io"
	"path"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/hatchify/errors"
	"github.com/mojura/kiroku"
)

const (
	// ErrInvalidName is returned when a name is empty
	ErrInvalidName = errors.Error("invalid name, cannot be empty")
	// ErrInvalidDirectory is returned when a directory is empty
	ErrInvalidDirectory = errors.Error("invalid directory, cannot be empty")
)

var _ kiroku.Source = &S3{}

func New(o Options) (sp *S3, err error) {
	var sess *session.Session
	cfg := o.makeConfig()
	if sess, err = session.NewSession(&cfg); err != nil {
		return
	}

	var s S3
	s.o = o
	s.s3 = s3.New(sess)

	if err = s.createBucket(); err != nil {
		return
	}

	sp = &s
	return
}

type S3 struct {
	o  Options
	s3 *s3.S3
}

func (s *S3) Export(ctx context.Context, prefix, filename string, r io.Reader) (newFilename string, err error) {
	rs, ok := r.(io.ReadSeeker)
	if !ok {
		rs = aws.ReadSeekCloser(r)
	}

	filepath := path.Join(prefix, filename)
	input := &s3.PutObjectInput{
		Bucket:               aws.String(s.o.Bucket),
		Key:                  aws.String(filepath),
		Body:                 rs,
		ServerSideEncryption: aws.String("AES256"),
		ACL:                  aws.String("private"),
	}

	if _, err = s.s3.PutObjectWithContext(ctx, input); err != nil {
		return
	}

	newFilename = filename
	return
}

func (s *S3) Import(ctx context.Context, prefix, filename string, w io.Writer) (err error) {
	filepath := path.Join(prefix, filename)
	getInput := s3.GetObjectInput{
		Bucket: aws.String(s.o.Bucket),
		Key:    aws.String(filepath),
	}

	var out *s3.GetObjectOutput
	if out, err = s.s3.GetObjectWithContext(ctx, &getInput); err != nil {
		return
	}
	defer out.Body.Close()

	_, err = io.Copy(w, out.Body)
	return
}

func (s *S3) Get(ctx context.Context, prefix, filename string, fn func(r io.Reader) error) (err error) {
	var out *s3.GetObjectOutput
	filepath := path.Join(prefix, filename)
	input := newGetInputObject(s.o.Bucket, filepath)
	if out, err = s.s3.GetObjectWithContext(ctx, input); err != nil {
		return handleError(err)
	}
	defer out.Body.Close()

	return fn(out.Body)
}

func (s *S3) GetNext(ctx context.Context, prefix, lastFilename string) (nextKey string, err error) {
	startAfter := path.Join(prefix, lastFilename)
	input := s3.ListObjectsV2Input{
		Bucket:     aws.String(s.o.Bucket),
		Prefix:     aws.String(prefix + "/"),
		StartAfter: aws.String(startAfter),
		MaxKeys:    aws.Int64(1),
	}

	var out *s3.ListObjectsV2Output
	if out, err = s.s3.ListObjectsV2WithContext(ctx, &input); err != nil {
		return
	}

	if len(out.Contents) == 0 {
		err = io.EOF
		return
	}

	nextKey = path.Base(*out.Contents[0].Key)
	return
}

func (s *S3) createBucket() (err error) {
	if s.o.AvoidBucketCreation {
		return
	}

	_, err = s.s3.CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String(s.o.Bucket),
		ACL:    aws.String("private"),
	})

	switch {
	case err == nil:
	case strings.Contains(err.Error(), s3.ErrCodeBucketAlreadyExists):
	case strings.Contains(err.Error(), s3.ErrCodeBucketAlreadyOwnedByYou):

	default:
		return
	}

	return nil
}

func (s *S3) deleteBucket() (err error) {
	if err = s.emptyBucket(); err != nil {
		return
	}

	input := &s3.DeleteBucketInput{
		Bucket: aws.String(s.o.Bucket),
	}

	_, err = s.s3.DeleteBucket(input)
	return
}

func (s *S3) emptyBucket() (err error) {
	// Setup BatchDeleteIterator to iterate through a list of objects.
	iter := s3manager.NewDeleteListIterator(s.s3, &s3.ListObjectsInput{
		Bucket: aws.String(s.o.Bucket),
	})

	// Initialize batcher
	batcher := s3manager.NewBatchDeleteWithClient(s.s3)

	// Traverse iterator deleting each object
	if err = batcher.Delete(aws.BackgroundContext(), iter); err != nil {
		return
	}

	return
}
