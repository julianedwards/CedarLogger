package logger

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sort"
	"sync"
	"time"

	"github.com/evergreen-ci/pail"
	"github.com/julianedwards/cedar/encode"
	"github.com/julianedwards/cedar/internal"
	"github.com/julianedwards/cedar/options"
	"github.com/mongodb/grip"
	"github.com/papertrail/go-tail/follower"
	"github.com/pkg/errors"
)

const (
	defaultMaxBufferSize int = 1e7
	defaultFlushInterval     = time.Minute
)

type bucketLogger struct {
	mu               sync.Mutex
	metaBucket       pail.Bucket
	logsBucket       pail.Bucket
	encodingRegistry encode.EncodingRegistry
}

func NewBucketLogger(ctx context.Context, opts options.Bucket) (*bucketLogger, error) {
	metaBucket, err := internal.CreateBucket(ctx, "metadata", opts)
	if err != nil {
		return nil, errors.Wrap(err, "creating metadata bucket")
	}
	logsBucket, err := internal.CreateBucket(ctx, "logs", opts)
	if err != nil {
		return nil, errors.Wrap(err, "creating logs bucket")
	}

	l := &bucketLogger{
		metaBucket:       metaBucket,
		logsBucket:       logsBucket,
		encodingRegistry: encode.GetGlobalRegistry(),
	}

	return l, nil

}

func (l *bucketLogger) AddMetadata(ctx context.Context, opts options.AddMetadata) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	keyWithExt, byteData, err := l.encode(opts.Data, opts.Key, opts.Encoding)
	if err != nil {
		return err
	}

	return errors.Wrap(l.metaBucket.Put(ctx, keyWithExt, bytes.NewReader(byteData)), "uploading metadata")
}

func (l *bucketLogger) Write(ctx context.Context, opts options.Write) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if err := opts.Validate(); err != nil {
		return err
	}

	keyWithExt, byteData, err := l.encode(opts.Data, opts.Key, opts.Encoding)
	if err != nil {
		return err
	}

	return errors.Wrap(l.logsBucket.Put(ctx, keyWithExt, bytes.NewReader(byteData)), "uploading data")
}

func (l *bucketLogger) WriteBytes(ctx context.Context, opts options.WriteBytes) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if err := opts.Validate(); err != nil {
		return err
	}

	e, err := l.getEncoding(opts.Encoding)
	if err != nil {
		return err
	}

	return errors.Wrap(l.logsBucket.Put(ctx, l.newKey(opts.Key, e.Extension()), bytes.NewReader(opts.Data)), "uploading data")
}

func (l *bucketLogger) FollowFile(ctx context.Context, opts options.FollowFile) error {
	if err := opts.Validate(); err != nil {
		return err
	}

	if opts.MaxBufferSize <= 0 {
		opts.MaxBufferSize = defaultMaxBufferSize
	}

	t, err := follower.New(opts.Filename, follower.Config{
		Whence: io.SeekEnd,
		Offset: 0,
		Reopen: true,
	})
	if err != nil {
		return errors.Wrap(err, "creating new file follower")
	}
	defer t.Close()

	var buffer []byte
	lines := t.Lines()
	catcher := grip.NewBasicCatcher()
	for {
		select {
		case line := <-lines:
			buffer = append(buffer, line.Bytes()...)
			if len(buffer) >= opts.MaxBufferSize {
				catcher.Add(l.WriteBytes(ctx, options.WriteBytes{
					Key:      opts.Key,
					Data:     buffer,
					Encoding: opts.Encoding,
				}))
				if catcher.HasErrors() {
					break
				}

				buffer = []byte{}
			}
		case <-opts.Exit:
			break
		case <-ctx.Done():
			catcher.Add(ctx.Err())
			break
		}
	}
	catcher.Wrap(t.Err(), "following log file")

	return catcher.Resolve()
}

func (l *bucketLogger) NewReadCloser(ctx context.Context, opts options.Read) (ReadCloser, error) {
	return l.newReadCloser(ctx, opts, false)
}

func (l *bucketLogger) NewReverseReadCloser(ctx context.Context, opts options.Read) (ReadCloser, error) {
	return l.newReadCloser(ctx, opts, true)
}

func (l *bucketLogger) newReadCloser(ctx context.Context, opts options.Read, reverse bool) (ReadCloser, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}

	bucket := l.logsBucket
	if opts.Metadata {
		bucket = l.metaBucket
	}

	r := &bucketReader{ctx: ctx, bucket: bucket}
	return r, r.getAndSortKeys(opts.Key, reverse)
}

func (l *bucketLogger) encode(data interface{}, prefix, encoding string) (string, []byte, error) {
	if prefix == "" {
		return "", nil, errors.New("must provide a key prefix")
	}

	e, err := l.getEncoding(encoding)
	if err != nil {
		return "", nil, err
	}

	out, err := e.Marshal(data)
	if err != nil {
		return "", nil, errors.Wrapf(err, "marshaling data to '%s'", e)
	}

	return l.newKey(prefix, e.Extension()), out, nil
}

func (l *bucketLogger) getEncoding(encoding string) (encode.Encoding, error) {
	if encoding == "" {
		encoding = encode.TEXT
	}

	e, ok := l.encodingRegistry.Get(encoding)
	if !ok {
		return nil, errors.Errorf("unrecognized encoding '%s'", encoding)
	}

	return e, nil
}

func (l *bucketLogger) newKey(prefix, ext string) string {
	key := fmt.Sprintf("%d", time.Now().UnixNano())
	if prefix != "" {
		key = prefix + "/" + key
	}
	if ext != "" {
		key += "." + ext
	}

	return key
}

type bucketReader struct {
	ctx    context.Context
	reader io.ReadCloser
	bucket pail.Bucket
	keys   []string
	keyIdx int
}

func (r *bucketReader) ReadPage() ([]byte, error) {
	if r.keyIdx == 0 {
		if err := r.getNextChunk(); err != nil {
			return nil, err
		}
	}

	if r.reader == nil {
		return nil, io.EOF
	}

	data, err := io.ReadAll(r.reader)
	return data, errors.Wrap(err, "reading next log page")
}

func (r *bucketReader) Read(p []byte) (int, error) {
	if r.keyIdx == 0 {
		if err := r.getNextChunk(); err != nil {
			return 0, err
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

func (r *bucketReader) getAndSortKeys(prefix string, reverse bool) error {
	it, err := r.bucket.List(r.ctx, prefix)
	if err != nil {
		return errors.Wrap(err, "listing log chunk keys")
	}

	for it.Next(r.ctx) {
		r.keys = append(r.keys, it.Item().Name())
	}
	if err = it.Err(); err != nil {
		return errors.Wrap(err, "iterating log chunk keys")
	}

	var sorter sort.Interface = sort.StringSlice(r.keys)
	if reverse {
		sorter = sort.Reverse(sorter)
	}
	sort.Sort(sorter)

	return nil
}

func (r *bucketReader) getNextChunk() error {
	if err := r.Close(); err != nil {
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
