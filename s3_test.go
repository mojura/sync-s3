package s3

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/mojura/kiroku"
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

func testExporterInit(name string) (e *Exporter, err error) {
	var o ExporterOptions
	o.Bucket = "mojura"
	o.Key = testKey
	o.Secret = testSecret
	o.Region = "us-west-1"
	o.Name = name
	return NewExporter(o)
}

func testImporterInit(ctx context.Context, dir, name string, updateInterval time.Duration) (i *Importer, err error) {
	var o ImporterOptions
	o.Bucket = "mojura"
	o.Key = testKey
	o.Secret = testSecret
	o.Region = "us-west-1"
	o.Dir = dir
	o.Name = name
	o.UpdateInterval = updateInterval
	return NewImporter(ctx, o)
}

func testClose(t *testing.T, s *S3) {
	if err := s.deleteBucket(); err != nil {
		t.Fatalf("Error encountered while deleting: %v\n", err)
	}
}

func TestExportImport(t *testing.T) {
	var (
		exp *Exporter
		imp *Importer
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

	if exp, err = testExporterInit("helloWorld"); err != nil {
		t.Fatal(err)
	}
	//	defer func() { _ = exp.s3.deleteBucket() }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(len(tcs))

	if imp, err = testImporterInit(ctx, "./test_data/import", "helloWorld", time.Second); err != nil {
		t.Fatal(err)
	}

	onImport := imp.OnImport()
	go func() {
		for range onImport {
			wg.Done()
		}
	}()

	// Populate
	for _, tc := range tcs {
		var w *kiroku.Writer
		if w, err = kiroku.NewWriter("./test_data", tc.name); err != nil {
			t.Fatal(err)
		}

		if err = w.AddBlock(kiroku.TypeWriteAction, []byte(tc.key), []byte(tc.value)); err != nil {
			t.Fatal(err)
		}

		tc.filename = w.Filename()

		if err = w.Close(); err != nil {
			t.Fatal(err)
		}

		if err = kiroku.Read(tc.filename, func(r *kiroku.Reader) (err error) {
			if err = exp.Export(r); err != nil {
				return
			}

			return
		}); err != nil {
			t.Fatal(err)
		}
	}

	wg.Wait()

	importedFilename := imp.k.Filename()

	var count int
	if err = kiroku.Read(importedFilename, func(r *kiroku.Reader) (err error) {
		return r.ForEach(0, func(b *kiroku.Block) (err error) {
			defer func() { count++ }()
			if count >= len(tcs) {
				return
			}

			tc := tcs[count]

			switch {
			case tc.key != string(b.Key):
				return fmt.Errorf("invalid key, expected <%s> and received <%s>", tc.key, string(b.Key))
			case tc.value != string(b.Value):
				return fmt.Errorf("invalid key, expected <%s> and received <%s>", tc.key, string(b.Key))
			}

			return
		})
	}); err != nil {
		t.Fatal(err)
	}

	if count != len(tcs) {
		t.Fatalf("invalid number of iterations, expected %d and received %d", len(tcs), count)
	}
}
