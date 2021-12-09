package logger

import (
	"bytes"
	"context"
	"fmt"
	"io"
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

	keyWithExt, byteData, err := l.encode(opts.Data, "metadata", opts.Encoding)
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
