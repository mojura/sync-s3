package s3

import "github.com/mojura/kiroku"

func NewExporter(o ExporterOptions) (ep *Exporter, err error) {
	var e Exporter
	if e.s3, err = New(o.Options); err != nil {
		return
	}

	e.o = o
	ep = &e
	return
}

type Exporter struct {
	s3 *S3

	o ExporterOptions
}

func (e *Exporter) Export(r *kiroku.Reader) (err error) {
	m := r.Meta()
	prefix := e.o.Name + "."
	filename := generateFilename(prefix, m.CreatedAt)
	rdr := r.ReadSeeker()
	return e.s3.Export(filename, rdr)
}
