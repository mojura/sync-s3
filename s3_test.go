package s3

import (
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
		"ay_0.txt",
		strings.NewReader("ayyyy 0!"),
	); err != nil {
		t.Fatal(err)
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

func TestExportImport(t *testing.T) {
	var (
		s   *S3
		err error
	)

	type testcase struct {
		name  string
		key   string
		value string

		filename string
	}

	tcs := []testcase{
		{
			name:  "helloWorld_0",
			key:   "0",
			value: "0_value",
		},
		{
			name:  "helloWorld_1",
			key:   "1",
			value: "1_value",
		},
		{
			name:  "helloWorld_2",
			key:   "2",
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
		if err = s.Export(tc.name, strings.NewReader(tc.value)); err != nil {
			t.Fatal(err)
		}
	}

	var nextKey string
	for i, tc := range tcs {
		if nextKey, err = s.GetNext(ctx, "", nextKey); err != nil {
			t.Fatalf("error during GetNext #%d: %v", i, err)
		}

		if nextKey != tc.name {
			t.Fatalf("invalid filename, expected <%s> and received <%s>", tc.name, nextKey)
		}
	}

	if _, err = s.GetNext(ctx, "", nextKey); err != io.EOF {
		t.Fatalf("invalid error, expected <%v> and received <%v>", io.EOF, err)
	}
}
