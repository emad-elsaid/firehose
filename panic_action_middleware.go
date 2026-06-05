package firehose

import (
	"context"
	"fmt"

	"github.com/emad-elsaid/boolexpr"
)

// StatusPanicRecovered indicates that a panic was recovered during action processing.
const StatusPanicRecovered Status = "Panic recovered"

// PanicActionMiddleware is an action middleware that recovers from panics during action processing
// and reports them with StatusPanicRecovered.
type PanicActionMiddleware[In, Out Event] struct {
	downstream Action[In, Out]
}

// Wrap stores the downstream action to be wrapped with panic recovery.
func (p *PanicActionMiddleware[In, Out]) Wrap(_ context.Context, rule Rule[In, Out], action Action[In, Out], in In) (Action[In, Out], error) {
	p.downstream = action

	return p, nil
}

// Process executes the downstream action with panic recovery, returning a Report with
// StatusPanicRecovered if a panic occurs.
func (p *PanicActionMiddleware[In, Out]) Process(ctx context.Context, event In, syms boolexpr.Symbols) (o Out, report Report) {
	defer func() {
		if r := recover(); r != nil {
			report = NewReport(StatusPanicRecovered, fmt.Errorf("%v", r))
		}
	}()

	return p.downstream.Process(ctx, event, syms)
}
