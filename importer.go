package s3

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"time"

	"github.com/gdbu/scribe"
	"github.com/mojura/kiroku"
)

var (
	defaultUpdateInterval = time.Second * 15
)

// NewImporter will return a new instance of Importer
func NewImporter(ctx context.Context, o ImporterOptions) (ip *Importer, err error) {
	var i Importer
	if i.s3, err = New(o.Options); err != nil {
		return
	}

	if i.k, err = kiroku.NewWriter(o.Dir, o.Name); err != nil {
		return
	}

	// Fill default values
	o.fill()

	prefix := fmt.Sprintf("Importer (%s)", o.Name)
	i.ctx = ctx
	i.out = scribe.New(prefix)
	i.o = o
	i.onImport = make(chan struct{}, 1)
	go i.watch()
	ip = &i
	return
}

type Importer struct {
	k   *kiroku.Writer
	s3  *S3
	out *scribe.Scribe
	ctx context.Context

	o ImporterOptions

	updateInterval time.Duration
	onImport       chan struct{}
}

// OnImport will return an onImport channel
func (i *Importer) OnImport() <-chan struct{} {
	return i.onImport
}

func (i *Importer) watch() {
	var (
		lastKey string
		err     error
	)

	prefix := i.o.Name + "."
	m := i.k.Meta()
	if m.CreatedAt > 0 {
		lastKey = generateFilename(prefix, m.CreatedAt)
	}

	for !i.isClosed() {
		var nextKey string
		nextKey, err = i.s3.GetNextKey(i.ctx, prefix, lastKey)
		switch err {
		case nil:
		case context.Canceled:
			return
		case io.EOF:
			time.Sleep(i.updateInterval)
			continue
		default:
			if strings.Contains(err.Error(), "context canceled") {
				return
			}

			i.out.Errorf("Error getting next key: %v. Sleeping for 1 minute", err)
			time.Sleep(time.Minute)
			continue
		}

		if err = i.process(nextKey); err != nil {
			i.out.Errorf("error processing <%s>: %v", nextKey, err)
			// TODO: Let's analyze which errors propagate through this
			// path and decide what the best course of action is
			continue
		}

		lastKey = nextKey

		select {
		case i.onImport <- struct{}{}:
			// Push to onImport channel
		default:
			// onImport channel is full, discard push
		}
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
	if f, err = ioutil.TempFile("", i.o.Name); err != nil {
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
