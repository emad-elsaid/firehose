// Package events defines event types for the firehose framework.
package events

import (
	"time"
)

// Time represents a time-based event.
type Time struct {
	Time time.Time
}

func (t Time) Seconds() int {
	return t.Time.Second()
}

func (t Time) Minutes() int {
	return t.Time.Minute()
}

func (t Time) Hours() int {
	return t.Time.Hour()
}
