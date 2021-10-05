package s3

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
)

var (
	testKey    = os.Getenv("S3_KEY")
	testSecret = os.Getenv("S3_SECRET")
)

func TestNew(t *testing.T) {
	var (
		s   *S3
		err error
	)

	if s, err = testInit(); err != nil {
		t.Fatal(err)
	}
	defer testClose(t, s)

}

func TestS3_Export(t *testing.T) {
	var (
		s   *S3
		err error
	)

	if s, err = testInit(); err != nil {
		t.Fatal(err)
	}
	defer testClose(t, s)

	if err = s.Export(
		context.Background(),
		"ay_0.txt",
		strings.NewReader("ayyyy 0!"),
	); err != nil {
		t.Fatal(err)
	}
}

func TestS3_Import(t *testing.T) {
	var (
		s   *S3
		err error
	)

	if s, err = testInit(); err != nil {
		t.Fatal(err)
	}
	defer testClose(t, s)

	if err = s.Export(
		context.Background(),
		"ay_0.txt",
		strings.NewReader("ayyyy 0!"),
	); err != nil {
		t.Fatal(err)
	}

	if err = s.Export(
		context.Background(),
		"helloWorld_0.txt",
		strings.NewReader("hello world 0!"),
	); err != nil {
		t.Fatal(err)
	}

	if err = s.Export(
		context.Background(),
		"helloWorld_1.txt",
		strings.NewReader("hello world 1!"),
	); err != nil {
		t.Fatal(err)
	}

	if err = s.Export(
		context.Background(),
		"helloWorld_2.txt",
		strings.NewReader("hello world 2!"),
	); err != nil {
		t.Fatal(err)
	}

	if err = s.Export(
		context.Background(),
		"zoop_0.txt",
		strings.NewReader("zoop! 0"),
	); err != nil {
		t.Fatal(err)
	}

	var (
		count   int
		nextKey string
	)

	for {
		buf := bytes.NewBuffer(nil)
		if nextKey, err = s.GetNext(context.Background(), "helloWorld", nextKey); err != nil {
			break
		}

		if err = s.Import(context.Background(), nextKey, buf); err != nil {
			break
		}

		var targetFilename, targetFileContents string
		switch count {
		case 0:
			targetFilename = "helloWorld_0.txt"
			targetFileContents = "hello world 0!"
		case 1:
			targetFilename = "helloWorld_1.txt"
			targetFileContents = "hello world 1!"
		case 2:
			targetFilename = "helloWorld_2.txt"
			targetFileContents = "hello world 2!"
		}

		if err = testAssertString(targetFilename, nextKey); err != nil {
			t.Fatalf("error with filename: %v", err)
		}

		if err = testAssertString(targetFileContents, buf.String()); err != nil {
			t.Fatalf("error with file contents: %v", err)
		}

		count++
	}

	switch err {
	case nil:
	case io.EOF:

	default:
		t.Fatal(err)
	}

	if count != 3 {
		t.Fatalf("invalid count, expected %d and received %d", 3, count)
	}
}

func testAssertString(a, b string) (err error) {
	switch {
	case len(a) == 0:
	case a != b:
		return fmt.Errorf("invalid nextKey, expected <%s> and received <%s>", a, b)
	}

	return nil
}

func testInit() (s *S3, err error) {
	var o Options
	o.Bucket = "mojura"
	o.Key = testKey
	o.Secret = testSecret
	o.Region = "us-west-1"
	return New(o)
}

func testClose(t *testing.T, s *S3) {
	if err := s.deleteBucket(context.Background()); err != nil {
		t.Fatalf("Error encountered while deleting: %v\n", err)
	}
}
