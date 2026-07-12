package destinations

import (
	"context"

	fh "github.com/emad-elsaid/firehose"
)

// ToSlice wraps a single item as a one-item slice and forwards it.
type ToSlice[T any] struct {
	Into fh.Destination[[]T] `validate:"required"`
}

// Send wraps event in a one-item slice and forwards it to Into.
func (t ToSlice[T]) Send(ctx context.Context, event T) error {
	if t.Into == nil {
		return fh.DestinationError{Err: ErrWrappedDestinationRequired}
	}

	return t.Into.Send(ctx, []T{event})
}
