package s3

// ExporterOptions represents the exporter options
type ExporterOptions struct {
	Options

	Name string `toml:"name" json:"name"`
}

// Validate will validat the exporter options
func (e *ExporterOptions) Validate() (err error) {
	if len(e.Name) == 0 {
		return ErrInvalidName
	}

	return
}
