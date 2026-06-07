package destinations

import (
	"context"
	"errors"
	"fmt"

	"github.com/emad-elsaid/firehose"
)

// ErrPanicRecovered is a static error for panic recovery in actions and destinations.
var ErrPanicRecovered = errors.New("action panicked")

// Panic is a destination middleware that recover from panics
// in the destination and reports them as a panic recovery.
type Panic[In, Out firehose.Event] struct {
	downstream firehose.Destination[Out]
}

// Wrap stores the downstream destination to be wrapped with panic recovery.
func (p *Panic[In, Out]) Wrap(
	_ context.Context,
	_ firehose.Rule[In, Out],
	destination firehose.Destination[Out],
	_ Out,
) (firehose.Destination[Out], error) {
	p.downstream = destination

	return p, nil
}

// Send executes the downstream destination with panic recovery, converting any panic into an error.
func (p *Panic[In, Out]) Send(ctx context.Context, event Out) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%w: %v", ErrPanicRecovered, r)
		}
	}()

	return p.downstream.Send(ctx, event)
}
