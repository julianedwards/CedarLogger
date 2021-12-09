package options

import (
	"github.com/mongodb/grip"
	"github.com/pkg/errors"
)

const defaultS3Region = "us-east-1"

type PailType string

const (
	PailS3    = "s3"
	PailLocal = "local"
)

func (t PailType) validate() error {
	switch t {
	case PailS3, PailLocal:
		return nil
	default:
		return errors.Errorf("unrecognized Pail type '%s'", t)
	}
}

type Bucket struct {
	Type PailType
	Name string
	S3   *S3Bucket
}

func (o *Bucket) Validate() error {
	catcher := grip.NewBasicCatcher()
	catcher.Add(o.Type.validate())
	catcher.NewWhen(o.Name == "", "must specify bucket name")

	switch o.Type {
	case PailS3:
		catcher.Add(o.S3.validate())
	}

	return catcher.Resolve()
}

type S3Bucket struct {
	Key    string
	Secret string
	Region string
}

func (o *S3Bucket) validate() error {
	if o == nil {
		return errors.New("must specify S3 bucket options")
	}

	catcher := grip.NewBasicCatcher()
	catcher.NewWhen(o.Key == "", "must specify AWS S3 key")
	catcher.NewWhen(o.Secret == "", "must specify AWS S3 secret")

	if o.Region == "" {
		o.Region = defaultS3Region
	}

	return catcher.Resolve()
}
