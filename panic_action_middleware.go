package firehose

import (
	"context"
	"fmt"

	"github.com/emad-elsaid/boolexpr"
)

const StatusPanicRecovered Status = "Panic recovered"

type PanicActionMiddleware[In, Out Event] struct {
	downstream Action[In, Out]
}

func (p *PanicActionMiddleware[In, Out]) Wrap(_ context.Context, rule Rule[In, Out], action Action[In, Out], in In) (Action[In, Out], error) {
	p.downstream = action

	return p, nil
}

func (p *PanicActionMiddleware[In, Out]) Process(ctx context.Context, event In, syms boolexpr.Symbols) (o Out, report Report) {
	defer func() {
		if r := recover(); r != nil {
			report = NewReport(StatusPanicRecovered, fmt.Errorf("%v", r))
		}
	}()

	return p.downstream.Process(ctx, event, syms)
}
