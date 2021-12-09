package options

import (
	"github.com/mongodb/grip"
)

type AddMetadata struct {
	Data     interface{}
	Encoding string
}

type Write struct {
	Key      string
	Data     interface{}
	Encoding string
}

func (o Write) Validate() error {
	catcher := grip.NewBasicCatcher()
	catcher.NewWhen(o.Key == "", "must specify a key")
	catcher.NewWhen(o.Data == nil, "data cannot be nil")

	return catcher.Resolve()
}

type WriteBytes struct {
	Key      string
	Data     []byte
	Encoding string
}

func (o WriteBytes) Validate() error {
	catcher := grip.NewBasicCatcher()
	catcher.NewWhen(o.Key == "", "must specify a key")
	catcher.NewWhen(o.Data == nil, "data cannot be nil")

	return catcher.Resolve()
}

type FollowFile struct {
	Key           string
	Filename      string
	Exit          chan struct{}
	Encoding      string
	MaxBufferSize int
}

func (o FollowFile) Validate() error {
	catcher := grip.NewBasicCatcher()
	catcher.NewWhen(o.Key == "", "must specify a key")
	catcher.NewWhen(o.Filename == "", "must specify a filename")
	catcher.NewWhen(o.Exit == nil, "exit channel cannot be nil")

	return catcher.Resolve()
}
