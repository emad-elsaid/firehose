package destinations

import (
	"context"
	"log/slog"

	"github.com/emad-elsaid/boolexpr"
	"github.com/emad-elsaid/firehose"
)

// Slog is a destination that writes events to standard output.
type Slog[T firehose.Event] struct {
	Message string
	Level   slog.Level
}

// Send writes the event using Go structured logging
func (s Slog[T]) Send(ctx context.Context, event T) error {
	attributes := event.Attributes(ctx)
	symbols := boolexpr.NewSymbolsCached(attributes)

	attrs := make([]any, 0, len(attributes)*2+2)
	attrs = append(attrs, "event", event)

	for k := range attributes {
		v, err := symbols.Get(k)
		if err != nil {
			attrs = append(attrs, k, err)
			continue
		}

		attrs = append(attrs, k, v)
	}

	slog.Log(ctx, s.Level, s.Message, attrs...)

	return nil
}
