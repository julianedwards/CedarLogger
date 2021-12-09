package internal

import (
	"context"

	"github.com/evergreen-ci/pail"
	"github.com/julianedwards/cedar/options"
	"github.com/pkg/errors"
)

func CreateBucket(ctx context.Context, prefix string, opts options.Bucket) (pail.Bucket, error) {
	if err := opts.Validate(); err != nil {
		return nil, errors.Wrap(err, "invalid bucket options")
	}

	var (
		bucket pail.Bucket
		err    error
	)
	switch opts.Type {
	case options.PailS3:
		bucket, err = pail.NewS3Bucket(pail.S3Options{
			Name:   opts.Name,
			Prefix: prefix,
			Region: opts.S3.Region,
			//Permissions: pail.S3Permissions(permissions),
			Credentials: pail.CreateAWSCredentials(opts.S3.Key, opts.S3.Secret, ""),
			MaxRetries:  10,
			Compress:    true,
		})
		if err != nil {
			return nil, errors.Wrap(err, "creating AWS S3 backed bucket")
		}
	default:
		bucket, err = pail.NewLocalBucket(pail.LocalOptions{
			Path:   opts.Name,
			Prefix: prefix,
		})
		if err != nil {
			return nil, errors.Wrap(err, "creating local filesystem backed bucket")
		}
	}

	return bucket, nil
}
