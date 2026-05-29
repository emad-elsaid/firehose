// Package events defines event types for the firehose framework.
package events

import (
	"context"
	"time"
)

// Time represents a time-based event.
type Time struct {
	Time time.Time
}

// Attributes returns a map of attributes for the Time event.
func (t Time) Attributes(_ context.Context) map[string]any {
	return map[string]any{
		"seconds": t.Time.Second(),
		"minute":  t.Time.Minute(),
		"hour":    t.Time.Hour(),
	}
}
