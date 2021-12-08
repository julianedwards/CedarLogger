package logger

import (
	"time"

	"github.com/mongodb/grip/level"
)

type LogLine struct {
	Timestamp      time.Time      `json:"ts"`
	Priority       level.Priority `json:"priority,omitempty"`
	PriorityString string         `json:"priority_string,omitempty"`
	Data           interface{}    `json:"data"`
}
