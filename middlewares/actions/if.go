package actions

import (
	"context"
	"fmt"
	"maps"
	"slices"

	"github.com/emad-elsaid/boolexpr"
	fh "github.com/emad-elsaid/firehose"
)

const ()

// If is an action middleware that conditionally executes actions based on boolean
// expressions evaluated against event attributes.
type If[I, O fh.Event] struct {
	rule            *fh.Rule[I, O]
	downstream      fh.Action[I, O]
	lastCondition   string
	parsedCondition boolexpr.Expression
}

// Wrap parses and validates the conditional expression from the rule, wrapping the downstream action
// to be executed only when the condition evaluates to true.
func (c *If[I, O]) Wrap(
	ctx context.Context,
	rule *fh.Rule[I, O],
	action fh.Action[I, O],
	inInstance I,
) (fh.Action[I, O], error) {
	if rule.If == "" {
		return action, nil
	}

	err := c.validateCondition(ctx, rule, inInstance)
	if err != nil {
		return nil, err
	}

	// Parse and cache the initial condition
	parsedIf, err := boolexpr.Parse(rule.If)
	if err != nil {
		return nil, err
	}

	c.rule = rule
	c.downstream = action
	c.lastCondition = rule.If
	c.parsedCondition = parsedIf

	return c, nil
}

// Process evaluates the conditional expression and processes the event through the downstream action
// only if the condition is true, otherwise returns an abort report with StatusNoMatch.
func (c *If[I, O]) Process(ctx context.Context, event I, syms boolexpr.Symbols) (O, fh.Report) {
	// If rule has no condition, always process
	if c.rule.If == "" {
		return c.downstream.Process(ctx, event, syms)
	}

	shouldProcess, err := c.shouldProcess(syms)
	if err != nil {
		var zero O

		return zero, fh.NewAbortReport(fh.StatusConditionError, err)
	}

	if !shouldProcess {
		var zero O

		return zero, fh.NewAbortReport(fh.StatusNoMatch, nil)
	}

	return c.downstream.Process(ctx, event, syms)
}

func (c *If[I, O]) shouldProcess(syms boolexpr.Symbols) (bool, error) {
	// Check if condition changed since last parse
	if c.rule.If != c.lastCondition {
		parsedIf, err := boolexpr.Parse(c.rule.If)
		if err != nil {
			return false, err
		}

		c.parsedCondition = parsedIf
		c.lastCondition = c.rule.If
	}

	shouldProcess, err := boolexpr.EvalExpression(c.parsedCondition, syms)
	if err != nil {
		return false, err
	}

	return shouldProcess, nil
}

func (c *If[I, O]) validateCondition(ctx context.Context, r *fh.Rule[I, O], instance I) error {
	parsedIf, err := boolexpr.Parse(r.If)
	if err != nil {
		return err
	}

	symsList := boolexpr.ListSymbols(parsedIf)

	attrs, err := fh.EventAttributes(ctx, instance)
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
