// Package sources provides event source implementations.
package sources

import (
	"context"
	"log/slog"
	"time"

	"github.com/emad-elsaid/firehose/events"
)

// Time is a source that emits events at regular intervals.
type Time struct {
	Period time.Duration
}

// ID returns a string identifier for this time source.
func (t Time) ID() string {
	return t.Period.String()
}

// Start begins emitting time events at the configured period.
func (t Time) Start(ctx context.Context, callback func(context.Context, events.Time) error) (context.Context, error) {
	done, cancel := context.WithCancel(ctx)
	ticker := time.NewTicker(t.Period)

	go func() {
		for {
			select {
			case now := <-ticker.C:
				err := callback(ctx, events.Time{Time: now})
				if err != nil {
					slog.Error("error in time event callback", "time", t, "error", err)
				}
			case <-ctx.Done():
			case <-done.Done():
				ticker.Stop()
				cancel()

				return
			}
		}
	}()

	return done, nil
}
