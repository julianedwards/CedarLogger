package logger

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/evergreen-ci/pail"
	"github.com/julianedwards/cedar"
	"github.com/mongodb/grip"
	"github.com/mongodb/grip/level"
	"github.com/mongodb/grip/message"
	"github.com/mongodb/grip/send"
	"github.com/papertrail/go-tail/follower"
	"github.com/pkg/errors"
)

const (
	defaultMaxBufferSize int = 1e7
	defaultFlushInterval     = time.Minute
)

type bucketLogger struct {
	mu         sync.Mutex
	ctx        context.Context
	cancel     context.CancelFunc
	buffer     []LogLine
	bufferSize int
	lastFlush  time.Time
	timer      *time.Timer
	closed     bool

	opts   BucketLoggerOptions
	sess   *cedar.BucketSession
	bucket pail.Bucket

	*send.Base
}

// LoggerOptions support the use and creation of a new bucket logger.
type BucketLoggerOptions struct {
	// Metadata is used to store any additonal data alongside the logs in
	// this bucket.
	Metadata interface{} `bson:"metadata" json:"metadata" yaml:"metadata"`

	// Local sender for "fallback" operations.
	Local send.Sender `bson:"-" json:"-" yaml:"-"`
	// LevelInfo is used to set the default and threshold logging levels.
	// This can be set at anytime but must be set at least once before any
	// calls to Send.
	LevelInfo *send.LevelInfo

	// MaxBufferSize is the maximum number of bytes to buffer before
	// flushing log data to bucket storage. Defaults to 10MB.
	MaxBufferSize int `bson:"max_buffer_size" json:"max_buffer_size" yaml:"max_buffer_size"`
	// FlushInterval is the interval at which to flush log data, regardless
	// of whether the max buffer size has been reached or not. Setting
	// FlushInterval to a duration less than 0 will disable timed flushes.
	// Defaults to 1 minute.
	FlushInterval time.Duration `bson:"flush_interval" json:"flush_interval" yaml:"flush_interval"`
}

func (opts *BucketLoggerOptions) validate() {
	if opts.Local == nil {
		opts.Local = send.MakeNative()
		opts.Local.SetName("local")
	}

	if opts.MaxBufferSize == 0 {
		opts.MaxBufferSize = defaultMaxBufferSize
	}

	if opts.FlushInterval == 0 {
		opts.FlushInterval = defaultFlushInterval
	}
}

func NewBucketLogger(sess *cedar.BucketSession, name string, opts BucketLoggerOptions) (*bucketLogger, error) {
	return NewBucketLoggerWithContext(context.Background(), sess, name, opts)
}

func NewBucketLoggerWithContext(ctx context.Context, sess *cedar.BucketSession, name string, opts BucketLoggerOptions) (*bucketLogger, error) {
	opts.validate()

	bucket, err := sess.Create(ctx, name)
	if err != nil {
		return nil, errors.Wrap(err, "creating Pail Bucket")
	}

	if opts.Metadata != nil {
		data, err := json.Marshal(opts.Metadata)
		if err != nil {
			return nil, errors.Wrap(err, "marshaling metadata to JSON")
		}

		if err = bucket.Put(ctx, "metadata", bytes.NewReader(data)); err != nil {
			return nil, errors.Wrap(err, "uploading metadata")
		}
	}

	logger := &bucketLogger{
		opts:   opts,
		sess:   sess,
		bucket: bucket,
		Base:   send.NewBase(name),
	}

	if err := logger.SetErrorHandler(send.ErrorHandlerFromSender(opts.Local)); err != nil {
		return nil, errors.Wrap(err, "setting default error handler")
	}

	if opts.LevelInfo != nil {
		if err = logger.SetLevel(*opts.LevelInfo); err != nil {
			return nil, errors.Wrap(err, "setting level")
		}
	}

	ctx, cancel := context.WithCancel(ctx)
	logger.ctx = ctx
	logger.cancel = cancel

	if opts.FlushInterval > 0 {
		go logger.timedFlush()
	}

	return logger, nil
}

func (l *bucketLogger) Write(ctx context.Context, data []byte) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	return errors.Wrap(l.bucket.Put(ctx, l.newKey(), bytes.NewReader(data)), "uploading data")
}

// TODO: Figure out how to signal that we should stop reading the log file.
// Maybe we can set Reopen to false and once the file is deleted, we know we
// can stop reading? If we want to enable rotating, we need to be careful about
// this.
func (l *bucketLogger) FollowFile(ctx context.Context, filename string) error {
	t, err := follower.New(filename, follower.Config{
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
			if len(buffer) >= l.opts.MaxBufferSize {
				l.Write(ctx, buffer)
				buffer = []byte{}
			}
		case <-ctx.Done():
			catcher.Add(ctx.Err())
			break
		}
	}

	catcher.Wrap(t.Err(), "following log file")
	return catcher.Resolve()
}

func (l *bucketLogger) Send(m message.Composer) {
	if !l.Level().ShouldLog(m) {
		return
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	if l.closed {
		l.opts.Local.Send(message.NewErrorMessage(level.Error, errors.New("cannot call Send on a closed bucket logger Sender")))
		return
	}

	l.buffer = append(l.buffer, LogLine{
		Timestamp:      time.Now(),
		Priority:       m.Priority(),
		PriorityString: m.Priority().String(),
		Data:           m.Raw(),
	})
	l.bufferSize += len(m.String())
	if l.bufferSize >= l.opts.MaxBufferSize {
		if err := l.flush(l.ctx); err != nil {
			l.opts.Local.Send(message.NewErrorMessage(level.Error, err))
			return
		}
	}
}

// Flush flushes anything data that may be in the buffer to bucket storage.
func (l *bucketLogger) Flush(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.closed {
		return nil
	}

	return l.flush(ctx)
}

// Close flushes anything that may be left in the underlying buffer and cleans
// up resources as necessary. Close is thread safe but should only be called
// once no more calls to Send are needed; after Close has been called any
// subsequent calls to Send will error. After the first call to Close
// subsequent calls will no-op.
func (l *bucketLogger) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	defer l.cancel()

	if l.closed {
		return nil
	}
	l.closed = true

	if len(l.buffer) > 0 {
		if err := l.flush(l.ctx); err != nil {
			l.opts.Local.Send(message.NewErrorMessage(level.Error, err))
			return errors.Wrap(err, "flushing buffer")
		}
	}

	return nil
}

func (l *bucketLogger) timedFlush() {
	l.mu.Lock()
	l.timer = time.NewTimer(l.opts.FlushInterval)
	l.mu.Unlock()
	defer l.timer.Stop()

	for {
		select {
		case <-l.ctx.Done():
			return
		case <-l.timer.C:
			l.mu.Lock()
			if len(l.buffer) > 0 && time.Since(l.lastFlush) >= l.opts.FlushInterval {
				if err := l.flush(l.ctx); err != nil {
					l.opts.Local.Send(message.NewErrorMessage(level.Error, err))
				}
			}
			_ = l.timer.Reset(l.opts.FlushInterval)
			l.mu.Unlock()
		}
	}
}

func (l *bucketLogger) flush(ctx context.Context) error {
	data, err := json.Marshal(l.buffer)
	if err != nil {
		return err
	}

	if err := l.bucket.Put(l.ctx, l.newKey(), bytes.NewReader(data)); err != nil {
		return err
	}

	l.buffer = []LogLine{}
	l.bufferSize = 0
	l.lastFlush = time.Now()

	return nil
}

func (l *bucketLogger) newKey() string {
	return fmt.Sprintf("%d", time.Now().UnixNano())
}
