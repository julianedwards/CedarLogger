package logger

import (
	"context"
	"io"

	"github.com/julianedwards/cedar/options"
)

type Logger interface {
	AddMetadata(context.Context, options.AddMetadata) error
	Write(context.Context, options.Write) error
	WriteBytes(context.Context, options.WriteBytes) error
	FollowFile(context.Context, options.FollowFile) error
	NewReadCloser(context.Context, options.Read) (io.ReadCloser, error)
	NewReverseReadCloser(context.Context, options.Read) (io.ReadCloser, error)
}
