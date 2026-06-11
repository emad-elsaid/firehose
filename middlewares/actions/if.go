package actions

import (
	"context"
	"fmt"
	"maps"
	"slices"

	"github.com/emad-elsaid/boolexpr"
	"github.com/emad-elsaid/firehose"
)

const ()

// If is an action middleware that conditionally executes actions based on boolean
// expressions evaluated against event attributes.
type If[In, Out firehose.Event] struct {
	parsedIf   *boolexpr.Expression
	downstream firehose.Action[In, Out]
}

// Wrap parses and validates the conditional expression from the rule, wrapping the downstream action
// to be executed only when the condition evaluates to true.
func (c *If[In, Out]) Wrap(
	ctx context.Context,
	rule firehose.Rule[In, Out],
	action firehose.Action[In, Out],
	inInstance In,
) (firehose.Action[In, Out], error) {
	if rule.If == "" {
		return action, nil
	}

	err := c.parseCondition(rule)
	if err != nil {
		return nil, err
	}

	err = c.isValidCondition(ctx, inInstance)
	if err != nil {
		return nil, err
	}

	c.downstream = action

	return c, nil
}

// Process evaluates the conditional expression and processes the event through the downstream action
// only if the condition is true, otherwise returns an abort report with StatusNoMatch.
func (c *If[In, Out]) Process(ctx context.Context, event In, syms boolexpr.Symbols) (Out, firehose.Report) {
	shouldProcess, err := c.shouldProcess(syms)
	if err != nil {
		var zero Out

		return zero, firehose.NewAbortReport(firehose.StatusConditionError, err)
	}

	if !shouldProcess {
		var zero Out

		return zero, firehose.NewAbortReport(firehose.StatusNoMatch, nil)
	}

	return c.downstream.Process(ctx, event, syms)
}

func (c *If[In, Out]) shouldProcess(syms boolexpr.Symbols) (bool, error) {
	shouldProcess, err := boolexpr.EvalExpression(*c.parsedIf, syms)
	if err != nil {
		return false, err
	}

	return shouldProcess, nil
}

func (c *If[In, Out]) parseCondition(r firehose.Rule[In, Out]) error {
	parsedIf, err := boolexpr.Parse(r.If)
	if err != nil {
		return err
	}

	c.parsedIf = &parsedIf

	return nil
}

func (c *If[In, Out]) isValidCondition(ctx context.Context, instance In) error {
	symsList := boolexpr.ListSymbols(*c.parsedIf)

	attrs, err := instance.Attributes(ctx)
	if err != nil {
		return fmt.Errorf("failed to get event attributes: %w", err)
	}

	attrsSyms := slices.Collect(maps.Keys(attrs))

	for _, sym := range symsList {
		if !slices.Contains(attrsSyms, sym) {
			return fmt.Errorf("%w: symbol: %s", boolexpr.ErrSymbolNotFound, sym)
		}
	}

	return nil
}
