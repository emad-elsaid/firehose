package destinations

import (
	"context"
	"errors"

	fh "github.com/emad-elsaid/firehose"
)

// ErrWrappedDestinationRequired is returned when a wrapper has no destination.
var ErrWrappedDestinationRequired = errors.New("wrapped destination is required")

// FromChan forwards each item from the input channel to the wrapped destination.
type FromChan[T any] struct {
	Into fh.Destination[T] `validate:"required"`
}

// Send forwards every item received from event to Into.
func (f FromChan[T]) Send(ctx context.Context, event chan T) error {
	if f.Into == nil {
		return fh.DestinationError{Err: ErrWrappedDestinationRequired}
	}

	var errs []error

	for item := range event {
		err := f.Into.Send(ctx, item)
		if err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return fh.DestinationError{Err: errors.Join(errs...)}
	}

	return nil
}
