package s3

import (
	"os"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/hatchify/errors"
)

func getErrorFirstLine(err error) (msg string) {
	spl := strings.SplitN(err.Error(), "\n", 2)
	return spl[0]
}

func getErrorCode(msg string) (code string) {
	spl := strings.SplitN(msg, ":", 2)
	return spl[0]
}

func newGetInputObject(bucket, filename string) *s3.GetObjectInput {
	var input s3.GetObjectInput
	input.Bucket = aws.String(bucket)
	input.Key = aws.String(filename)
	return &input
}

func newHeadObjectInput(bucket, filename string) *s3.HeadObjectInput {
	var input s3.HeadObjectInput
	input.Bucket = aws.String(bucket)
	input.Key = aws.String(filename)
	return &input
}

func handleError(err error) error {
	msg := getErrorFirstLine(err)
	code := getErrorCode(msg)
	switch code {
	case s3.ErrCodeNoSuchKey:
		return os.ErrNotExist

	default:
		return errors.New(msg)
	}
}

func getPtrValue[T any](in *T) (out T) {
	if in == nil {
		return
	}

	return *in
}
