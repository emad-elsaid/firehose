package destinations

import (
	"context"
	"errors"

	fh "github.com/emad-elsaid/firehose"
)

// FromSlice forwards each item from the input slice to the wrapped destination.
type FromSlice[T any] struct {
	Into fh.Destination[T] `validate:"required"`
}

// Send forwards every item received from event to Into.
func (f FromSlice[T]) Send(ctx context.Context, event []T) fh.Report {
	if f.Into == nil {
		return fh.NewReport(fh.DestinationError{Err: ErrWrappedDestinationRequired})
	}

	var errs []error

	for _, item := range event {
		report := f.Into.Send(ctx, item)
		if report.Err != nil {
			errs = append(errs, report.Err)
		}
	}

	if len(errs) > 0 {
		return fh.NewReport(fh.DestinationError{Err: errors.Join(errs...)})
	}

	return fh.NewSuccessReport()
}
