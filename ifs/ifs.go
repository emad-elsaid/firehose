package ifs

import (
	"context"

	"github.com/emad-elsaid/boolexpr"
	"github.com/emad-elsaid/firehose"
)

// Ifs is a slice of If conditions that are evaluated sequentially.
// If any condition returns false or an error, evaluation stops and returns that result.
type Ifs[I any] []firehose.If[I]

// Evaluate evaluates all conditions in sequence. Returns false on the first
// condition that fails, or true if all conditions pass.
func (ifs Ifs[I]) Evaluate(ctx context.Context, event I, syms boolexpr.Symbols) (bool, error) {
	for _, cond := range ifs {
		pass, err := cond.Evaluate(ctx, event, syms)
		if err != nil {
			return false, err
		}

		if !pass {
			return false, nil
		}
	}

	return true, nil
}
