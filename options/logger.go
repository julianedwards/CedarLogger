package options

import (
	"time"

	"github.com/mongodb/grip/send"
)

type Sender struct {
	Key string

	// Local sender for "fallback" operations.
	Local send.Sender `bson:"-" json:"-" yaml:"-"`
	// LevelInfo is used to set the default and threshold logging levels.
	// This can be set at anytime but must be set at least once before any
	// calls to Send.
	LevelInfo *send.LevelInfo

	// MaxBufferSize is the maximum number of bytes to buffer before
	// flushing data.
	MaxBufferSize int `bson:"max_buffer_size" json:"max_buffer_size" yaml:"max_buffer_size"`
	// FlushInterval is the interval at which to flush data, regardless of
	// whether the max buffer size has been reached or not. Setting
	// FlushInterval to a duration less than 0 will disable timed flushes.
	FlushInterval time.Duration `bson:"flush_interval" json:"flush_interval" yaml:"flush_interval"`
}
