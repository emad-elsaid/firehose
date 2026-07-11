package condition

import (
	"context"

	"github.com/emad-elsaid/boolexpr"
	"github.com/emad-elsaid/firehose"
)

// Conditions is a slice of condition checks that are evaluated sequentially.
// If any condition returns false or an error, evaluation stops and returns that result.
type Conditions[I any] []firehose.Condition[I]

// Evaluate evaluates all conditions in sequence. Returns false on the first
// condition that fails, or true if all conditions pass.
func (conds Conditions[I]) Evaluate(ctx context.Context, event I, syms boolexpr.Symbols) (bool, error) {
	for _, cond := range conds {
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
