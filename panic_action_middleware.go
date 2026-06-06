package firehose

import (
	"context"
	"fmt"

	"github.com/emad-elsaid/boolexpr"
)

// PanicActionMiddleware is an action middleware that recovers from panics during action processing
// and reports them with StatusPanicRecovered.
type PanicActionMiddleware[In, Out Event] struct {
	downstream Action[In, Out]
}

// Wrap stores the downstream action to be wrapped with panic recovery.
func (p *PanicActionMiddleware[In, Out]) Wrap(
	_ context.Context,
	_ Rule[In, Out],
	action Action[In, Out],
	_ In,
) (Action[In, Out], error) {
	p.downstream = action

	return p, nil
}

// Process executes the downstream action with panic recovery, returning a Report with
// StatusPanicRecovered if a panic occurs.
func (p *PanicActionMiddleware[In, Out]) Process(
	ctx context.Context,
	event In,
	syms boolexpr.Symbols,
) (Out, Report) {
	var output Out

	var report Report

	defer func() {
		if r := recover(); r != nil {
			report = NewReport(StatusPanicRecovered, fmt.Errorf("%w: %v", ErrPanicRecovered, r))
		}
	}()

	output, report = p.downstream.Process(ctx, event, syms)

	return output, report
}
