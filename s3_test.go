package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
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
		"ay_0.txt",
		strings.NewReader("ayyyy 0!"),
	); err != nil {
		t.Fatal(err)
	}
}

func TestS3_ImportNext(t *testing.T) {
	var (
		s   *S3
		err error
	)

	if s, err = testInit(); err != nil {
		t.Fatal(err)
	}
	defer testClose(t, s)

	if err = s.Export(
		"ay_0.txt",
		strings.NewReader("ayyyy 0!"),
	); err != nil {
		t.Fatal(err)
	}

	if err = s.Export(
		"helloWorld_0.txt",
		strings.NewReader("hello world 0!"),
	); err != nil {
		t.Fatal(err)
	}

	if err = s.Export(
		"helloWorld_1.txt",
		strings.NewReader("hello world 1!"),
	); err != nil {
		t.Fatal(err)
	}

	if err = s.Export(
		"helloWorld_2.txt",
		strings.NewReader("hello world 2!"),
	); err != nil {
		t.Fatal(err)
	}

	if err = s.Export(
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
		buf := aws.NewWriteAtBuffer(nil)
		if nextKey, err = s.ImportNext(context.Background(), "helloWorld", nextKey, buf); err != nil {
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

		if err = testAssertString(targetFileContents, string(buf.Bytes())); err != nil {
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
	if err := s.deleteBucket(); err != nil {
		t.Fatalf("Error encountered while deleting: %v\n", err)
	}
}
