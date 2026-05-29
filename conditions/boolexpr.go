// Package conditions provides a set of utilities to evaluate conditions based on event.
package conditions

import (
	"context"

	"github.com/emad-elsaid/boolexpr"
)

// Attributable is an interface that events can implement to provide attributes for condition evaluation.
type Attributable interface {
	Attributes(ctx context.Context) map[string]any
}

// BoolExpr is a wrapper around boolexpr.Expr to implement the Cond interface.
type BoolExpr[T Attributable] string

// Eval evaluates the boolean expression against the provided event's attributes.
func (b BoolExpr[T]) Eval(ctx context.Context, event T) (bool, error) {
	if b == "" {
		return true, nil
	}

	expr, err := boolexpr.Parse(string(b))
	if err != nil {
		return false, err
	}

	vars := boolexpr.NewSymbolsCached(event.Attributes(ctx))

	result, err := boolexpr.EvalExpression(expr, vars)
	if err != nil {
		return false, err
	}

	return result, nil
}
