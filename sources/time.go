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
func (t Time) Start(ctx context.Context, callback timeCallback) (context.Context, error) {
	done, cancel := context.WithCancel(ctx)
	ticker := time.NewTicker(t.Period)

	go t.tick(ctx, done, cancel, ticker, callback)

	return done, nil
}

func (t Time) tick(ctx, done context.Context, cancel context.CancelFunc, ticker *time.Ticker, callback timeCallback) {
	for {
		select {
		case now := <-ticker.C:
			t.emit(ctx, now, callback)
		case <-ctx.Done():
		case <-done.Done():
			ticker.Stop()
			cancel()

			return
		}
	}
}

func (t Time) emit(ctx context.Context, now time.Time, callback timeCallback) {
	err := callback(ctx, events.Time(now))
	if err != nil {
		slog.Error("error in time event callback", "time", t, "error", err)
	}
}

type timeCallback = func(context.Context, events.Time) error
