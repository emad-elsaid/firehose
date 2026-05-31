// Package events defines event types for the firehose framework.
package events

import (
	"context"
	"time"
)

// Time represents a time-based event.
type Time time.Time

// Attributes returns a map of attributes for the Time event.
func (t Time) Attributes(_ context.Context) map[string]any {
	return map[string]any{
		"seconds": time.Time(t).Second(),
		"minute":  time.Time(t).Minute(),
		"hour":    time.Time(t).Hour(),
	}
}

func (t Time) String() string {
	return time.Time(t).String()
}
