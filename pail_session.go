package cedar

import (
	"context"

	"github.com/evergreen-ci/pail"
	"github.com/julianedwards/cedar/internal"
	"github.com/mongodb/grip"
	"github.com/pkg/errors"
)

const defaultS3Region = "us-east-1"

type BucketSession struct {
	internal.BucketSession
}

type s3BucketSession struct {
	opts S3BucketOptions
}

type S3BucketOptions struct {
	AWSKey    string
	AWSSecret string
	Region    string
	Bucket    string
}

func (o *S3BucketOptions) validate() error {
	catcher := grip.NewBasicCatcher()

	catcher.NewWhen(o.AWSKey == "", "must provide an AWS key")
	catcher.NewWhen(o.AWSSecret == "", "must provide an AWS secret")

	if o.Region == "" {
		o.Region = defaultS3Region
	}

	return catcher.Resolve()
}

func NewS3BucketSession(opts S3BucketOptions) (*BucketSession, error) {
	if err := opts.validate(); err != nil {
		return nil, errors.Wrap(err, "invalid S3 bucket session options")
	}

	return &BucketSession{
		BucketSession: &s3BucketSession{opts: opts},
	}, nil
}

func (s *s3BucketSession) Create(ctx context.Context, prefix string) (pail.Bucket, error) {
	bucket, err := pail.NewS3Bucket(pail.S3Options{
		Name:   s.opts.Bucket,
		Prefix: prefix,
		Region: s.opts.Region,
		//Permissions: pail.S3Permissions(permissions),
		Credentials: pail.CreateAWSCredentials(s.opts.AWSKey, s.opts.AWSSecret, ""),
		MaxRetries:  10,
		Compress:    true,
	})
	if err != nil {
		return nil, errors.Wrap(err, "creating S3 backed Pail Bucket")
	}

	return bucket, nil
}

type localBucketSession struct {
	bucket string
}

func NewLocalBucketSession(bucket string) *BucketSession {
	return &BucketSession{
		BucketSession: &localBucketSession{bucket: bucket},
	}
}

func (s *localBucketSession) Create(ctx context.Context, prefix string) (pail.Bucket, error) {
	bucket, err := pail.NewLocalBucket(pail.LocalOptions{
		Path:   s.bucket,
		Prefix: prefix,
	})
	if err != nil {
		return nil, errors.Wrap(err, "creating local backed Pail Bucket")
	}

	return bucket, nil
}
