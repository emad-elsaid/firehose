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
func (c Chain5[I, A, B, C, D, O]) Process(ctx context.Context, event I, syms boolexpr.Symbols) (O, error) {
	firstOut, err := c.First.Process(ctx, event, syms)
	if err != nil {
		var zero O

		return zero, err
	}

	secondOut, err := c.Second.Process(ctx, firstOut, syms)
	if err != nil {
		var zero O

		return zero, err
	}

	thirdOut, err := c.Third.Process(ctx, secondOut, syms)
	if err != nil {
		var zero O

		return zero, err
	}

	fourthOut, err := c.Fourth.Process(ctx, thirdOut, syms)
	if err != nil {
		var zero O

		return zero, err
	}

	return c.Fifth.Process(ctx, fourthOut, syms)
}
