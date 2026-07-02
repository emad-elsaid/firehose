package ifs

import (
	"context"

	"github.com/emad-elsaid/boolexpr"
	"github.com/emad-elsaid/memoize"
)

var memoizedBoolExprParse = memoize.NewWithErr(boolexpr.Parse)

// Cond is a string-based condition that evaluates boolean expressions against event attributes.
type Cond[I any] string

// Evaluate parses and evaluates the boolean expression against the provided symbols.
func (c Cond[I]) Evaluate(_ context.Context, _ I, syms boolexpr.Symbols) (bool, error) {
	if c == "" {
		return true, nil
	}

	expr, err := memoizedBoolExprParse(string(c))
	if err != nil {
		return false, err
	}

	return boolexpr.EvalExpression(expr, syms)
}
