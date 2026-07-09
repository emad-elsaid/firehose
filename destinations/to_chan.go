package destinations

import (
	"context"

	fh "github.com/emad-elsaid/firehose"
)

// ToChan wraps a single item as a one-item channel and forwards it.
type ToChan[T any] struct {
	Into fh.Destination[chan T] `validate:"required"`
}

// Send wraps event in a one-item channel and forwards it to Into.
func (t ToChan[T]) Send(ctx context.Context, event T) fh.Report {
	if t.Into == nil {
		return fh.NewReport(fh.DestinationError{Err: ErrWrappedDestinationRequired})
	}

	items := make(chan T, 1)
	items <- event

	close(items)

	return t.Into.Send(ctx, items)
}
