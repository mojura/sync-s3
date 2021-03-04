package s3

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
)

type Options struct {
	Key    string `toml:"key" json:"key"`
	Secret string `toml:"secret" json:"secret"`

	Endpoint string `toml:"endpoint" json:"endpoint"`
	Region   string `toml:"region" json:"region"`

	Bucket string `toml:"bucket" json:"bucket"`
}

func (o *Options) makeConfig() (cfg aws.Config) {
	cfg.Credentials = credentials.NewStaticCredentials(o.Key, o.Secret, "")

	if len(o.Endpoint) > 0 {
		cfg.Endpoint = aws.String(o.Endpoint)
	}

	if len(o.Region) > 0 {
		cfg.Region = aws.String(o.Region)
	}

	return
}
