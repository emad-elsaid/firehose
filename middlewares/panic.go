// Package middlewares provides reusable firehose middleware implementations.
package middlewares

import (
	"context"
	"errors"
	"fmt"

	"github.com/emad-elsaid/boolexpr"
	"github.com/emad-elsaid/firehose"
)

// ErrPanicRecovered is a static error for panic recovery.
var ErrPanicRecovered = errors.New("panic recovered")

// Panic is a middleware that recovers from panics in callbacks, actions, and destinations.
type Panic[I, O any] struct {
	downstreamCallback firehose.Callback[I]
	downstreamAction   firehose.Action[I, O]
	downstreamDest     firehose.Destination[O]
}

// WrapCallback stores the downstream callback to be wrapped with panic recovery.
func (p *Panic[I, O]) WrapCallback(
	_ context.Context,
	_ *firehose.Rule[I, O],
	callback firehose.Callback[I],
) (firehose.Callback[I], error) {
	p.downstreamCallback = callback

	return p.recoverCallback, nil
}

// WrapAction stores the downstream action to be wrapped with panic recovery.
func (p *Panic[I, O]) WrapAction(
	_ context.Context,
	_ *firehose.Rule[I, O],
	action firehose.Action[I, O],
) (firehose.Action[I, O], error) {
	p.downstreamAction = action

	return p, nil
}

// WrapDestination stores the downstream destination to be wrapped with panic recovery.
func (p *Panic[I, O]) WrapDestination(
	_ context.Context,
	_ *firehose.Rule[I, O],
	destination firehose.Destination[O],
) (firehose.Destination[O], error) {
	p.downstreamDest = destination

	return p, nil
}

func (p *Panic[I, O]) recoverCallback(ctx context.Context, event I, report firehose.ReportFunc) {
	defer func() {
		if recovered := recover(); recovered != nil {
			report(firehose.NewReport(fmt.Errorf("%w: %v", ErrPanicRecovered, recovered)))
		}
	}()

	p.downstreamCallback(ctx, event, report)
}

// Process executes the downstream action with panic recovery.
//
//nolint:nonamedreturns // Named returns allow defer to modify return values on panic recovery
func (p *Panic[I, O]) Process(
	ctx context.Context,
	event I,
	syms boolexpr.Symbols,
) (output O, report firehose.Report) {
	defer func() {
		if recovered := recover(); recovered != nil {
			var zero O

			output = zero
			report = firehose.NewReport(fmt.Errorf("%w: %v", ErrPanicRecovered, recovered))
		}
	}()

	output, report = p.downstreamAction.Process(ctx, event, syms)

	return output, report
}

// Send executes the downstream destination with panic recovery.
//
//nolint:nonamedreturns // Named return allows defer to modify return value on panic recovery
func (p *Panic[I, O]) Send(ctx context.Context, event O) (report firehose.Report) {
	defer func() {
		if recovered := recover(); recovered != nil {
			report = firehose.NewReport(fmt.Errorf("%w: %v", ErrPanicRecovered, recovered))
		}
	}()

	report = p.downstreamDest.Send(ctx, event)

	return report
}
