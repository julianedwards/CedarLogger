package logger

import (
	"context"
	"io"
	"sort"

	"github.com/evergreen-ci/pail"
	"github.com/julianedwards/cedar/session"
	"github.com/pkg/errors"
)

type bucketReader struct {
	ctx    context.Context
	reader io.ReadCloser
	bucket pail.Bucket
	keys   []string
	keyIdx int
}

func NewBucketLogReadCloser(ctx context.Context, sess *session.BucketSession, name string) (io.ReadCloser, error) {
	bucket, err := sess.Create(ctx, name)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	reader := &bucketReader{ctx: ctx, bucket: bucket}
	return reader, reader.getAndSortKeys()
}

func (r *bucketReader) Read(p []byte) (int, error) {
	if r.keyIdx == 0 {
		if err := r.getNextChunk(); err != nil {
			return 0, errors.WithStack(err)
		}
	}

	if r.reader == nil {
		return 0, io.EOF
	}

	var (
		n      int
		offset int
		err    error
	)
	for offset < len(p) {
		n, err = r.reader.Read(p[offset:])
		offset += n
		if err == io.EOF {
			err = r.getNextChunk()
		}
		if err != nil {
			break
		}
	}

	return offset, err
}

func (r *bucketReader) Close() error {
	if r.reader == nil {
		return nil
	}

	return errors.WithStack(r.reader.Close())
}

func (r *bucketReader) getAndSortKeys() error {
	it, err := r.bucket.List(r.ctx, "")
	if err != nil {
		return errors.Wrap(err, "listing log chunk keys")
	}

	for it.Next(r.ctx) {
		r.keys = append(r.keys, it.Item().Name())
	}
	if err = it.Err(); err != nil {
		return errors.Wrap(err, "iterating log chunk keys")
	}

	sort.Strings(r.keys)

	return nil
}

func (r *bucketReader) getNextChunk() error {
	if err := r.reader.Close(); err != nil {
		return errors.Wrap(err, "closing previous ReadCloser")
	}
	r.reader = nil

	if r.keyIdx == len(r.keys) {
		return nil
	}

	reader, err := r.bucket.Get(r.ctx, r.keys[r.keyIdx])
	if err != nil {
		return errors.Wrap(err, "getting next log chunk")
	}

	r.reader = reader
	r.keyIdx++

	return nil
}
