// Package sources provides event source implementations.
package sources

import (
	"context"
	"time"

	"github.com/emad-elsaid/firehose"
)

// Time is a source that emits events at regular intervals.
type Time struct {
	Period time.Duration
}

// Start begins emitting time events at the configured period.
func (t Time) Start(ctx context.Context, callback firehose.Callback[time.Time]) (context.Context, error) {
	done, cancel := context.WithCancel(ctx)
	ticker := time.NewTicker(t.Period)
	t.tick(ctx, done, cancel, ticker, callback)

	return done, nil
}

func (t Time) tick(
	ctx, done context.Context,
	cancel context.CancelFunc,
	ticker *time.Ticker,
	callback firehose.Callback[time.Time],
) {
	reports := make(chan firehose.Report)
	for {
		select {
		case now := <-ticker.C:
			callback(ctx, now, reports)
		case <-ctx.Done():
		case <-done.Done():
			ticker.Stop()
			cancel()

			return
		case <-reports:
		}
	}
}
