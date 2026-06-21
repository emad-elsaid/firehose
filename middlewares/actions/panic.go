package actions

import (
	"context"
	"errors"
	"fmt"

	"github.com/emad-elsaid/boolexpr"
	"github.com/emad-elsaid/firehose"
)

// StatusPanicRecovered returned when an action panics.
const StatusPanicRecovered firehose.Status = "Action Panicked"

// ErrPanicRecovered is a static error for panic recovery in actions and destinations.
var ErrPanicRecovered = errors.New("action panicked")

// Panic is an action middleware that recovers from panics during action processing
// and reports them with StatusPanicRecovered.
type Panic[I, O firehose.Event] struct {
	downstream firehose.Action[I, O]
}

// Wrap stores the downstream action to be wrapped with panic recovery.
func (p *Panic[I, O]) Wrap(
	_ context.Context,
	_ firehose.Rule[I, O],
	action firehose.Action[I, O],
	_ I,
) (firehose.Action[I, O], error) {
	p.downstream = action

	return p, nil
}

// Process executes the downstream action with panic recovery, returning a Report with
// StatusPanicRecovered if a panic occurs.
//
//nolint:nonamedreturns // Named returns allow defer to modify return values on panic recovery
func (p *Panic[I, O]) Process(
	ctx context.Context,
	event I,
	syms boolexpr.Symbols,
) (output O, report firehose.Report) {
	defer func() {
		if r := recover(); r != nil {
			var zero O

			output = zero
			report = firehose.NewAbortReport(StatusPanicRecovered, fmt.Errorf("%w: %v", ErrPanicRecovered, r))
		}
	}()

	output, report = p.downstream.Process(ctx, event, syms)

	return output, report
}
