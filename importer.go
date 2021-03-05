package s3

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"time"

	"github.com/gdbu/scribe"
	"github.com/mojura/kiroku"
)

var (
	defaultUpdateInterval = time.Second * 15
)

// NewImporter will return a new instance of Importer
func NewImporter(dir, name string, o ImporterOptions) (ip *Importer, err error) {
	var i Importer
	if i.s3, err = New(o.Options); err != nil {
		return
	}

	if i.k, err = kiroku.NewWriter(dir, name); err != nil {
		return
	}

	// Fill default values
	o.fill()

	prefix := fmt.Sprintf("Importer (%s)", name)
	i.out = scribe.New(prefix)
	i.name = name
	ip = &i
	return
}

type Importer struct {
	k   *kiroku.Writer
	s3  *S3
	out *scribe.Scribe
	ctx context.Context

	name string

	updateInterval time.Duration
	onUpdate       func()
}

func (i *Importer) Watch() {
	var (
		nextKey string
		err     error
	)

	prefix := i.name + "."
	m := i.k.Meta()
	if m.CreatedAt > 0 {
		nextKey = generateFilename(prefix, m.CreatedAt)
	}

	for !i.isClosed() {
		nextKey, err = i.s3.GetNextKey(i.ctx, prefix, nextKey)
		switch err {
		case nil:
		case io.EOF:
			time.Sleep(i.updateInterval)
		default:
			i.out.Errorf("Error getting next key: %v. Sleeping for 1 minute", err)
			time.Sleep(time.Minute)
		}

		if err = i.process(nextKey); err != nil {
			i.out.Errorf("error processing <%s>: %v", nextKey, err)
			// TODO: Let's analyze which errors propagate through this
			// path and decide what the best course of action is
			continue
		}

		i.onUpdate()
	}
}

func (i *Importer) isClosed() bool {
	select {
	case <-i.ctx.Done():
		return true
	default:
		return false
	}
}

func (i *Importer) process(nextKey string) (err error) {
	var f *os.File
	if f, err = ioutil.TempFile("", i.name); err != nil {
		return
	}

	filename := f.Name()
	defer os.Remove(filename)
	defer f.Close()

	if err = i.s3.Import(context.Background(), nextKey, f); err != nil {
		return
	}

	var r *kiroku.Reader
	if r, err = kiroku.NewReader(f); err != nil {
		return
	}

	return i.k.Merge(r)
}
