package actions

import (
	"context"

	"github.com/emad-elsaid/boolexpr"
	fh "github.com/emad-elsaid/firehose"
)

// Chain5 composes five actions.
type Chain5[I, A, B, C, D, O any] struct {
	First  fh.Action[I, A] `validate:"required"`
	Second fh.Action[A, B] `validate:"required"`
	Third  fh.Action[B, C] `validate:"required"`
	Fourth fh.Action[C, D] `validate:"required"`
	Fifth  fh.Action[D, O] `validate:"required"`
}

// Process runs the chain in order.
func (c Chain5[I, A, B, C, D, O]) Process(ctx context.Context, event I, syms boolexpr.Symbols) (O, fh.Report) {
	firstOut, report := c.First.Process(ctx, event, syms)
	if report.Err != nil {
		var zero O

		return zero, report
	}

	secondOut, report := c.Second.Process(ctx, firstOut, syms)
	if report.Err != nil {
		var zero O

		return zero, report
	}

	thirdOut, report := c.Third.Process(ctx, secondOut, syms)
	if report.Err != nil {
		var zero O

		return zero, report
	}

	fourthOut, report := c.Fourth.Process(ctx, thirdOut, syms)
	if report.Err != nil {
		var zero O

		return zero, report
	}

	return c.Fifth.Process(ctx, fourthOut, syms)
}
