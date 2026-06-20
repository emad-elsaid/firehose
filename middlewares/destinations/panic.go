package destinations

import (
	"context"
	"errors"
	"fmt"

	"github.com/emad-elsaid/firehose"
)

// StatusPanicRecovered returned when an destination panics.
const StatusPanicRecovered firehose.Status = "Destination Panicked"

// ErrPanicRecovered is a static error for panic recovery in actions and destinations.
var ErrPanicRecovered = errors.New("action panicked")

// Panic is a destination middleware that recover from panics
// in the destination and reports them as a panic recovery.
type Panic[I, O firehose.Event] struct {
	downstream firehose.Destination[O]
}

// Wrap stores the downstream destination to be wrapped with panic recovery.
func (p *Panic[I, O]) Wrap(
	_ context.Context,
	_ firehose.Rule[I, O],
	destination firehose.Destination[O],
	_ O,
) (firehose.Destination[O], error) {
	p.downstream = destination

	return p, nil
}

// Send executes the downstream destination with panic recovery, converting any panic into an error.
func (p *Panic[I, O]) Send(ctx context.Context, event O) (report firehose.Report) {
	defer func() {
		if r := recover(); r != nil {
			report = firehose.NewAbortReport(StatusPanicRecovered, fmt.Errorf("%w: %v", ErrPanicRecovered, r))
		}
	}()

	report = p.downstream.Send(ctx, event)

	return report
}
