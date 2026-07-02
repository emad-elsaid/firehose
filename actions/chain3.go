package actions

import (
	"context"

	"github.com/emad-elsaid/boolexpr"
	fh "github.com/emad-elsaid/firehose"
)

// Chain3 composes three actions.
type Chain3[I, A, B, O any] struct {
	First  fh.Action[I, A] `validate:"required"`
	Second fh.Action[A, B] `validate:"required"`
	Third  fh.Action[B, O] `validate:"required"`
}

// Process runs the chain in order.
func (c Chain3[I, A, B, O]) Process(ctx context.Context, event I, syms boolexpr.Symbols) (O, fh.Report) {
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

	return c.Third.Process(ctx, secondOut, syms)
}
