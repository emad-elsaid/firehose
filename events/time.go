// Package events defines event types for the firehose framework.
package events

import "time"

// Time represents a time-based event.
type Time struct {
	Time time.Time
}
