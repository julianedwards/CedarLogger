package logger

import (
	"context"

	"github.com/mongodb/grip/send"
)

type Logger interface {
	Write(context.Context, string, []byte) error
	FollowFile(context.Context, string, chan struct{}) error

	send.Sender
}
