package s3

import "time"

// ImporterOptions represents the importer options
type ImporterOptions struct {
	Options

	Name string `toml:"name" json:"name"`
	Dir  string `toml:"dir" json:"dir"`

	UpdateInterval time.Duration `toml:"updateInterval" json:"updateInterval"`
}

func (o *ImporterOptions) fill() {
	if o.UpdateInterval == 0 {
		o.UpdateInterval = defaultUpdateInterval
	}
}

// Validate will validat the exporter options
func (e *ImporterOptions) Validate() (err error) {
	if len(e.Name) == 0 {
		return ErrInvalidName
	}

	if len(e.Dir) == 0 {
		return ErrInvalidDirectory
	}

	return
}
