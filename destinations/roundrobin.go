package destinations

import (
	"context"
	"sync"

	fh "github.com/emad-elsaid/firehose"
)

// RoundRobin forwards events to destinations in sequence.
type RoundRobin[T any] struct {
	Destinations []fh.Destination[T] `validate:"required,min=1,dive,required"`

	mutex sync.Mutex
	next  int
}

// Send forwards the event to the next destination.
func (r *RoundRobin[T]) Send(ctx context.Context, event T) fh.Report {
	if len(r.Destinations) == 0 {
		return fh.NewReport(fh.DestinationError{Err: ErrNoDestinationsConfigured})
	}

	r.mutex.Lock()
	index := r.next % len(r.Destinations)
	r.next = (r.next + 1) % len(r.Destinations)
	r.mutex.Unlock()

	return r.Destinations[index].Send(ctx, event)
}
