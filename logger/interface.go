package logger

import (
	"context"

	"github.com/julianedwards/cedar/options"
)

type Logger interface {
	AddMetadata(context.Context, options.AddMetadata) error
	Write(context.Context, options.Write) error
	WriteBytes(context.Context, options.WriteBytes) error
	FollowFile(context.Context, options.FollowFile) error
}
