package destinations

import (
	"context"
	"errors"

	fh "github.com/emad-elsaid/firehose"
)

// ErrNoDestinationsConfigured is returned when a destination fanout or dispatcher
// has no downstream destinations.
var ErrNoDestinationsConfigured = errors.New("no destinations configured")

// Fanout forwards events to all configured destinations.
//
// If one or more destinations fail, their errors are joined and returned as
// DestinationError.
type Fanout[T any] struct {
	Destinations []fh.Destination[T] `validate:"required,min=1,dive,required"`
}

// Send forwards the event to all destinations.
func (f Fanout[T]) Send(ctx context.Context, event T) fh.Report {
	if len(f.Destinations) == 0 {
		return fh.NewReport(fh.DestinationError{Err: ErrNoDestinationsConfigured})
	}

	var errs []error

	for _, destination := range f.Destinations {
		report := destination.Send(ctx, event)
		if report.Err != nil {
			errs = append(errs, report.Err)
		}
	}

	if len(errs) > 0 {
		return fh.NewReport(fh.DestinationError{Err: errors.Join(errs...)})
	}

	return fh.NewSuccessReport()
}
