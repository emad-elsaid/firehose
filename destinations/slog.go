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

// Send writes the event using Go structured logging.
func (s Slog[T]) Send(ctx context.Context, event T) firehose.Report {
	attributes, err := event.Attributes(ctx)
	if err != nil {
		return firehose.NewReport(firehose.StatusDestinationError, err)
	}

	symbols := boolexpr.NewSymbolsCached(attributes)

	const eventAttrsCount = 2

	const pair = 2

	attrs := make([]any, 0, len(attributes)*pair+eventAttrsCount)
	attrs = append(attrs, "event", event)

	for key := range attributes {
		value, err := symbols.Get(key)
		if err != nil {
			attrs = append(attrs, key, err)

			continue
		}

		attrs = append(attrs, key, value)
	}

	slog.Log(ctx, s.Level, s.Message, attrs...)

	return firehose.NewReport(firehose.StatusSuccess, nil)
}
