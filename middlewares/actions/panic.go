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
type Panic[In, Out firehose.Event] struct {
	downstream firehose.Action[In, Out]
}

// Wrap stores the downstream action to be wrapped with panic recovery.
func (p *Panic[In, Out]) Wrap(
	_ context.Context,
	_ firehose.Rule[In, Out],
	action firehose.Action[In, Out],
	_ In,
) (firehose.Action[In, Out], error) {
	p.downstream = action

	return p, nil
}

// Process executes the downstream action with panic recovery, returning a Report with
// StatusPanicRecovered if a panic occurs.
func (p *Panic[In, Out]) Process(
	ctx context.Context,
	event In,
	syms boolexpr.Symbols,
) (Out, firehose.Report) {
	var output Out

	var report firehose.Report

	defer func() {
		if r := recover(); r != nil {
			report = firehose.NewAbortReport(StatusPanicRecovered, fmt.Errorf("%w: %v", ErrPanicRecovered, r))
		}
	}()

	output, report = p.downstream.Process(ctx, event, syms)

	return output, report
}
