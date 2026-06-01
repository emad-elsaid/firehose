package destinations

import (
	"context"
	"log/slog"
)

// Slog is a destination that writes events to standard output.
type Slog[T any] struct {
	Message string
	Level   slog.Level
}

// Send writes the event using Go structured logging
func (s Slog[T]) Send(ctx context.Context, event T) error {
	slog.Log(ctx, s.Level, s.Message, "event", event)

	return nil
}
