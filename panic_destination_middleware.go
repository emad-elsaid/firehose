package firehose

import (
	"context"
	"fmt"
)

// PanicDestinationMiddleware is a destination middleware that recover from panics
// in the destination and reports them as a panic recovery.
type PanicDestinationMiddleware[In, Out Event] struct {
	downstream Destination[Out]
}

// Wrap stores the downstream destination to be wrapped with panic recovery.
func (p *PanicDestinationMiddleware[In, Out]) Wrap(
	_ context.Context,
	_ Rule[In, Out],
	destination Destination[Out],
	_ Out,
) (Destination[Out], error) {
	p.downstream = destination

	return p, nil
}

// Send executes the downstream destination with panic recovery, converting any panic into an error.
func (p *PanicDestinationMiddleware[In, Out]) Send(ctx context.Context, event Out) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%w: %v", ErrPanicRecovered, r)
		}
	}()

	return p.downstream.Send(ctx, event)
}
