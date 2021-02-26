package main

import (
	"context"
	"io"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

func New(o Options) (sp *S3, err error) {
	var sess *session.Session
	cfg := o.makeConfig()
	if sess, err = session.NewSession(&cfg); err != nil {
		return
	}

	var s S3
	s.o = o
	s.s3 = s3.New(sess)
	s.d = s3manager.NewDownloader(sess)
	s.u = s3manager.NewUploader(sess)

	if err = s.createBucket(); err != nil {
		return
	}

	sp = &s
	return
}

type S3 struct {
	o  Options
	s3 *s3.S3
	d  *s3manager.Downloader
	u  *s3manager.Uploader
}

func (s *S3) Export(filename string, r io.Reader) (err error) {
	input := &s3.PutObjectInput{
		Bucket:               aws.String(s.o.Bucket),
		Key:                  aws.String(filename),
		Body:                 aws.ReadSeekCloser(r),
		ServerSideEncryption: aws.String("AES256"),
		ACL:                  aws.String("private"),
	}

	_, err = s.s3.PutObject(input)
	return
}

func (s *S3) ImportNext(ctx context.Context, prefix, lastFilename string, w io.WriterAt) (filename string, err error) {
	if filename, err = s.getNextKey(ctx, prefix, lastFilename); err != nil {
		return
	}

	getInput := s3.GetObjectInput{
		Bucket: aws.String(s.o.Bucket),
		Key:    aws.String(filename),
	}

	_, err = s.d.DownloadWithContext(ctx, w, &getInput)
	return
}

func (s *S3) createBucket() (err error) {
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

func (s *S3) getNextKey(ctx context.Context, prefix, lastFilename string) (nextKey string, err error) {
	input := s3.ListObjectsV2Input{
		Bucket:     aws.String(s.o.Bucket),
		Prefix:     aws.String(prefix),
		StartAfter: aws.String(lastFilename),
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

	nextKey = *out.Contents[0].Key
	return
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
