package firehose

import (
	"context"
	"fmt"
)

// PanicDestinationMiddleware is a destination middleware that recover from panics in the destination and reports them as a panic recovery.
type PanicDestinationMiddleware[In, Out Event] struct {
	downstream Destination[Out]
}

func (p *PanicDestinationMiddleware[In, Out]) Wrap(_ context.Context, rule Rule[In, Out], destination Destination[Out], in Out) (Destination[Out], error) {
	p.downstream = destination

	return p, nil
}

func (p *PanicDestinationMiddleware[In, Out]) Send(ctx context.Context, event Out) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("Panic recovered %v", r)
		}
	}()

	return p.downstream.Send(ctx, event)
}
