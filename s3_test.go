package s3

import (
	"context"
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
		"ay_0.txt",
		strings.NewReader("ayyyy 0!"),
	); err != nil {
		t.Fatal(err)
	}
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

func TestExportImport(t *testing.T) {
	var (
		s   *S3
		err error
	)

	type testcase struct {
		key   string
		value string
	}

	tcs := []testcase{
		{
			key:   "helloWorld_0",
			value: "0_value",
		},
		{
			key:   "helloWorld_1",
			value: "1_value",
		},
		{
			key:   "helloWorld_2",
			value: "2_value",
		},
	}

	if err = os.MkdirAll("./test_data/import", 0744); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll("./test_data")

	if s, err = testInit(); err != nil {
		t.Fatal(err)
	}
	//defer func() { _ = s.deleteBucket() }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Populate
	for _, tc := range tcs {
		if err = s.Export(tc.key, strings.NewReader(tc.value)); err != nil {
			t.Fatal(err)
		}
	}

	var nextKey string
	for i, tc := range tcs {
		if nextKey, err = s.GetNext(ctx, "", nextKey); err != nil {
			t.Fatalf("error during GetNext #%d: %v", i, err)
		}

		if nextKey != tc.key {
			t.Fatalf("invalid filename, expected <%s> and received <%s>", tc.key, nextKey)
		}
	}

	if _, err = s.GetNext(ctx, "", nextKey); err != io.EOF {
		t.Fatalf("invalid error, expected <%v> and received <%v>", io.EOF, err)
	}
}
