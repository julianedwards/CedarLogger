package logger

import (
	"context"
	"sync"
	"time"

	"github.com/julianedwards/cedar/encode"
	"github.com/julianedwards/cedar/options"
	"github.com/mongodb/grip/level"
	"github.com/mongodb/grip/message"
	"github.com/mongodb/grip/send"
	"github.com/pkg/errors"
)

type sender struct {
	mu         sync.Mutex
	ctx        context.Context
	cancel     context.CancelFunc
	buffer     []LogLine
	bufferSize int
	lastFlush  time.Time
	timer      *time.Timer
	closed     bool

	opts   options.Sender
	logger Logger

	*send.Base
}

func NewSender(ctx context.Context, logger Logger, opts options.Sender) (*sender, error) {
	s := &sender{
		opts:   opts,
		logger: logger,
		Base:   send.NewBase(opts.Key),
	}

	if err := s.SetErrorHandler(send.ErrorHandlerFromSender(opts.Local)); err != nil {
		return nil, errors.Wrap(err, "setting default error handler")
	}

	if opts.LevelInfo != nil {
		if err := s.SetLevel(*opts.LevelInfo); err != nil {
			return nil, errors.Wrap(err, "setting level")
		}
	}

	ctx, cancel := context.WithCancel(ctx)
	s.ctx = ctx
	s.cancel = cancel

	if opts.MaxBufferSize <= 0 {
		opts.MaxBufferSize = defaultMaxBufferSize
	}
	if opts.FlushInterval > 0 {
		go s.timedFlush()
	}

	return s, nil
}

func (s *sender) Send(m message.Composer) {
	if !s.Level().ShouldLog(m) {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		s.opts.Local.Send(message.NewErrorMessage(level.Error, errors.New("cannot call Send on a closed bucket logger Sender")))
		return
	}

	s.buffer = append(s.buffer, LogLine{
		Timestamp:      time.Now(),
		Priority:       m.Priority(),
		PriorityString: m.Priority().String(),
		Data:           m.Raw(),
	})
	s.bufferSize += len(m.String())
	if s.bufferSize >= s.opts.MaxBufferSize {
		if err := s.flush(s.ctx); err != nil {
			s.opts.Local.Send(message.NewErrorMessage(level.Error, err))
			return
		}
	}
}

// Flush flushes anything data that may be in the buffer to bucket storage.
func (s *sender) Flush(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}

	return s.flush(ctx)
}

// Close flushes anything that may be left in the underlying buffer and cleans
// up resources as necessary. Close is thread safe but should only be called
// once no more calls to Send are needed; after Close has been called any
// subsequent calls to Send will error. After the first call to Close
// subsequent calls will no-op.
func (s *sender) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	defer s.cancel()

	if s.closed {
		return nil
	}
	s.closed = true

	if len(s.buffer) > 0 {
		if err := s.flush(s.ctx); err != nil {
			s.opts.Local.Send(message.NewErrorMessage(level.Error, err))
			return errors.Wrap(err, "flushing buffer")
		}
	}

	return nil
}

func (s *sender) timedFlush() {
	s.mu.Lock()
	s.timer = time.NewTimer(s.opts.FlushInterval)
	s.mu.Unlock()
	defer s.timer.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-s.timer.C:
			s.mu.Lock()
			if len(s.buffer) > 0 && time.Since(s.lastFlush) >= s.opts.FlushInterval {
				if err := s.flush(s.ctx); err != nil {
					s.opts.Local.Send(message.NewErrorMessage(level.Error, err))
				}
			}
			_ = s.timer.Reset(s.opts.FlushInterval)
			s.mu.Unlock()
		}
	}
}

func (s *sender) flush(ctx context.Context) error {
	err := s.logger.Write(s.ctx, options.Write{
		Key:      s.opts.Key,
		Data:     s.buffer,
		Encoding: encode.JSON,
	})
	if err != nil {
		return err
	}

	s.buffer = []LogLine{}
	s.bufferSize = 0
	s.lastFlush = time.Now()

	return nil
}
