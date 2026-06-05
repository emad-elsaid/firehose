package firehose

import (
	"context"
	"fmt"
	"maps"
	"slices"

	"github.com/emad-elsaid/boolexpr"
)

const (
	StatusConditionError Status = "Condition error"
	StatusNoMatch        Status = "No match"
)

type IfActionMiddleware[In, Out Event] struct {
	If         string
	parsedIf   *boolexpr.Expression
	downstream Action[In, Out]
}

func (c *IfActionMiddleware[In, Out]) Wrap(ctx context.Context, rule Rule[In, Out], action Action[In, Out], in In) (Action[In, Out], error) {
	if rule.If == "" {
		return action, nil
	}

	err := c.parseCondition(rule)
	if err != nil {
		return nil, err
	}

	err = c.isValidCondition(ctx, &rule, in)
	if err != nil {
		return nil, err
	}

	c.downstream = action

	return c, nil
}

func (c *IfActionMiddleware[In, Out]) Process(ctx context.Context, event In, syms boolexpr.Symbols) (Out, Report) {
	shouldProcess, err := c.shouldProcess(syms)
	if err != nil {
		var zero Out
		return zero, NewReport(StatusConditionError, err)
	}

	if !shouldProcess {
		var zero Out
		return zero, NewAbortReport(StatusNoMatch, nil)
	}

	return c.downstream.Process(ctx, event, syms)
}

func (c *IfActionMiddleware[In, Out]) shouldProcess(syms boolexpr.Symbols) (bool, error) {
	shouldProcess, err := boolexpr.EvalExpression(*c.parsedIf, syms)
	if err != nil {
		return false, err
	}

	return shouldProcess, nil
}

func (c *IfActionMiddleware[In, Out]) parseCondition(r Rule[In, Out]) error {
	parsedIf, err := boolexpr.Parse(r.If)
	if err != nil {
		return err
	}

	c.parsedIf = &parsedIf

	return nil
}

func (c *IfActionMiddleware[In, Out]) isValidCondition(ctx context.Context, rule *Rule[In, Out], instance In) error {
	symsList := boolexpr.ListSymbols(*c.parsedIf)
	attrs := slices.Collect(maps.Keys(instance.Attributes(ctx)))

	for _, sym := range symsList {
		if !slices.Contains(attrs, sym) {
			return fmt.Errorf("%w: symbol: %s", boolexpr.ErrSymbolNotFound, sym)
		}
	}

	return nil
}
