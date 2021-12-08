package internal

import (
	"context"

	"github.com/evergreen-ci/pail"
)

type BucketSession interface {
	Create(context.Context, string) (pail.Bucket, error)
}
