// Package sources provides event source implementations.
package sources

import (
	"context"
	"log/slog"
	"time"

	"github.com/emad-elsaid/firehose"
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
func (t Time) Start(ctx context.Context, callback firehose.SourceCallback[events.Time]) (context.Context, error) {
	done, cancel := context.WithCancel(ctx)
	ticker := time.NewTicker(t.Period)

	go t.tick(ctx, done, cancel, ticker, callback)

	return done, nil
}

func (t Time) tick(ctx, done context.Context, cancel context.CancelFunc, ticker *time.Ticker, callback firehose.SourceCallback[events.Time]) {
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

func (t Time) emit(ctx context.Context, now time.Time, callback firehose.SourceCallback[events.Time]) {
	reports := callback(ctx, events.Time(now))
	for report := range reports {
		if report.Err != nil {
			slog.Error("error in time event callback", "time", t, "error", report.Err)
		}
	}
}
