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
	s          *S3
)

func TestNew(t *testing.T) {
	var (
		err error
	)

	if s, err = testInit(); err != nil {
		t.Fatal(err)
	}
	defer testClose(t, s)

}

func TestS3_Export(t *testing.T) {
	var (
		err error
	)

	//if s, err = testInit(); err != nil {
	//	t.Fatal(err)
	//}
	defer testClose(t, s)

	var gotKey string
	if gotKey, err = s.Export(
		context.Background(),
		"testing",
		"ay_0.txt",
		strings.NewReader("ayyyy 0!"),
	); err != nil {
		t.Fatal(err)
	}

	if gotKey != "ay_0.txt" {
		t.Fatalf("invalid newFilename, expected = <%v> and received = <%v>", "ay_0.txt", gotKey)
	}
}

func TestExportImport(t *testing.T) {
	var (
		err error
	)

	type testcase struct {
		prefix string
		key    string
		value  string
	}

	tcs := []testcase{
		{
			prefix: "testingExportImport",
			key:    "helloWorld_0",
			value:  "0_value",
		},
		{
			prefix: "testingExportImport",
			key:    "helloWorld_1",
			value:  "1_value",
		},
		{
			prefix: "testingExportImport",
			key:    "helloWorld_2",
			value:  "2_value",
		},
	}

	if err = os.MkdirAll("./test_data/import", 0744); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll("./test_data")

	//	if s, err = testInit(); err != nil {
	//		t.Fatal(err)
	//}

	defer func() { _ = s.deleteBucket() }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Populate
	for _, tc := range tcs {
		var gotKey string
		if gotKey, err = s.Export(context.Background(), tc.prefix, tc.key, strings.NewReader(tc.value)); err != nil {
			t.Fatal(err)
		}

		if gotKey != tc.key {
			t.Fatalf("invalid newFilename, expected = <%v> and received = <%v>", tc.key, gotKey)
		}
	}

	var nextKey string
	for i, tc := range tcs {
		if nextKey, err = s.GetNext(ctx, tc.prefix, nextKey); err != nil {
			t.Fatalf("error during GetNext #%d: %v", i, err)
		}

		if nextKey != tc.key {
			t.Fatalf("invalid filename, expected <%s> and received <%s>", tc.key, nextKey)
		}
	}

	if _, err = s.GetNext(ctx, "", nextKey); err != io.EOF {
		t.Fatalf("invalid error, expected <%v> and received <%v>", io.EOF, err)
	}

	var nextList []string
	if nextList, err = s.GetNextList(ctx, tcs[0].prefix, tcs[0].key, int64(len(tcs))); err != nil {
		t.Fatalf("error during GetNextList #%v: %v", tcs[0].key, err)
	}

	if len(nextList) != len(tcs)-1 {
		t.Fatalf("error during GetNextList: got wrong size")
	}
}

func testInit() (s *S3, err error) {
	var o Options
	o.Bucket = "mojura-sync-s3dev"
	o.Key = testKey
	o.Secret = testSecret
	o.Region = "us-east-1"
	//o.AvoidBucketCreation = true
	o.Endpoint = "https://nyc3.digitaloceanspaces.com"
	return New(o)
}

func testClose(t *testing.T, s *S3) {
	//if err := s.deleteBucket(); err != nil {
	//	t.Fatalf("Error encountered while deleting: %v\n", err)
	//}
}
