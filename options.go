package s3

import "github.com/aws/aws-sdk-go-v2/aws"

type Options struct {
	Key    string `toml:"key" json:"key"`
	Secret string `toml:"secret" json:"secret"`

	Endpoint string `toml:"endpoint" json:"endpoint"`
	Region   string `toml:"region" json:"region"`

	Bucket string `toml:"bucket" json:"bucket"`
}

// ResolveEndpoint allows Options to match the aws.EndpointResolver interface
func (o *Options) ResolveEndpoint(key, service string) (endpoint aws.Endpoint, err error) {
	endpoint.URL = o.Endpoint
	return
}
