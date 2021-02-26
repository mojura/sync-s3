package s3

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
)

type Options struct {
	Key    string `json:"key"`
	Secret string `json:"secret"`

	Endpoint string `json:"endpoint"`
	Region   string `json:"region"`

	Bucket string `json:"bucket"`
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
