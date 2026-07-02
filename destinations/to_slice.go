package destinations

import (
	"context"

	fh "github.com/emad-elsaid/firehose"
)

// ToSlice wraps a single item as a one-item slice and forwards it.
type ToSlice[T any] struct {
	To fh.Destination[[]T] `validate:"required"`
}

// Send wraps event in a one-item slice and forwards it to To.
func (t ToSlice[T]) Send(ctx context.Context, event T) fh.Report {
	if t.To == nil {
		return fh.NewReport(fh.DestinationError{Err: ErrWrappedDestinationRequired})
	}

	return t.To.Send(ctx, []T{event})
}
