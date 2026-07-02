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
	To fh.Destination[T] `validate:"required"`
}

// Send forwards every item received from event to To.
func (f FromChan[T]) Send(ctx context.Context, event chan T) fh.Report {
	if f.To == nil {
		return fh.NewReport(fh.DestinationError{Err: ErrWrappedDestinationRequired})
	}

	var errs []error

	for item := range event {
		report := f.To.Send(ctx, item)
		if report.Err != nil {
			errs = append(errs, report.Err)
		}
	}

	if len(errs) > 0 {
		return fh.NewReport(fh.DestinationError{Err: errors.Join(errs...)})
	}

	return fh.NewSuccessReport()
}
