package actions

import (
	"context"

	"github.com/emad-elsaid/boolexpr"
	fh "github.com/emad-elsaid/firehose"
)

// Chain composes two actions.
//
// The output of First is passed as input to Second.
type Chain[I, M, O any] struct {
	First  fh.Action[I, M] `validate:"required"`
	Second fh.Action[M, O] `validate:"required"`
}

// Process runs the chain in order.
func (c Chain[I, M, O]) Process(ctx context.Context, event I, syms boolexpr.Symbols) (O, error) {
	mid, err := c.First.Process(ctx, event, syms)
	if err != nil {
		var zero O

		return zero, err
	}

	return c.Second.Process(ctx, mid, syms)
}
