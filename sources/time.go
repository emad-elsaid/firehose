package sources

import (
	"context"
	"log/slog"
	"time"

	"github.com/emad-elsaid/firehose/events"
)

type Time struct {
	Period time.Duration
}

func (t Time) ID() string {
	return t.Period.String()
}

func (t Time) Start(ctx context.Context, cb func(context.Context, events.Time) error) (context.Context, error) {
	done, cancel := context.WithCancel(ctx)
	ticker := time.NewTicker(t.Period)

	go func() {
		for {
			select {
			case now := <-ticker.C:
				err := cb(ctx, events.Time{Time: now})
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
