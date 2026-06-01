// Package firehose provides an event processing pipeline framework.
package firehose

import (
	"context"
	"errors"
	"fmt"

	"github.com/emad-elsaid/boolexpr"
)

// ErrIncompatibleSource is returned when the next rule in the same source chain doesn't have the same source type.
var ErrIncompatibleSource = errors.New("next rule doesn't have the same source")

// Rule defines an event processing pipeline from source to destination.
type Rule[In, Out Event] struct {
	When Source[In] `validate:"required"`
	If   string
	Then Action[In, Out]  `validate:"required"`
	To   Destination[Out] `validate:"required"`

	next, prev                     Registry
	nextSameSource, prevSameSource sourceRegistry

	ctx      context.Context
	parsedIf *boolexpr.Expression
}

func (r *Rule[In, Out]) getNext() Registry                  { return r.next }
func (r *Rule[In, Out]) setNext(n Registry)                 { r.next = n }
func (r *Rule[In, Out]) getPrev() Registry                  { return r.prev }
func (r *Rule[In, Out]) setPrev(p Registry)                 { r.prev = p }
func (r *Rule[In, Out]) setNextSameSource(n sourceRegistry) { r.nextSameSource = n }
func (r *Rule[In, Out]) setPrevSameSource(p sourceRegistry) { r.prevSameSource = p }
func (r *Rule[In, Out]) getSourceRegistry() sourceRegistry  { return r }
func (r *Rule[In, Out]) getRegistry() Registry              { return r }
func (r *Rule[In, Out]) getCtx() context.Context            { return r.ctx }
func (r *Rule[In, Out]) getSource() any                     { return r.When }

func (r *Rule[In, Out]) start(ctx context.Context) error {
	isFirstSameSource := r.prevSameSource == nil
	if !isFirstSameSource {
		return nil
	}

	ctx, err := r.When.Start(ctx, r.callback)
	if err != nil {
		return fmt.Errorf("failed to start source: %w", err)
	}

	r.ctx = ctx

	return nil
}

func (r *Rule[In, Out]) callback(ctx context.Context, event In) error {
	return r.callbackWithSyms(ctx, event, nil)
}

func (r *Rule[In, Out]) callbackWithSyms(ctx context.Context, event In, syms boolexpr.Symbols) error {
	if r.parsedIf != nil && syms == nil {
		syms = boolexpr.NewSymbolsCached(event.Attributes(ctx))
	}

	err := r.run(ctx, event, syms)
	if err != nil {
		return fmt.Errorf("error processing event in rule with source %T: %w", r.When, err)
	}

	return r.callNextRule(ctx, event, syms)
}

func (r *Rule[In, Out]) callNextRule(ctx context.Context, event In, syms boolexpr.Symbols) error {
	if r.nextSameSource == nil {
		return nil
	}

	callbackable, ok := r.nextSameSource.getRegistry().(callbackable[In])
	if !ok {
		return fmt.Errorf("%w: current rule %#v, next %#v", ErrIncompatibleSource, r, r.nextSameSource)
	}

	return callbackable.callbackWithSyms(ctx, event, syms)
}

func (r *Rule[In, Out]) run(ctx context.Context, event In, syms boolexpr.Symbols) error {
	shouldProcess, err := r.shouldProcess(syms)
	if err != nil {
		return err
	}

	if !shouldProcess {
		return nil
	}

	out, err := r.Then.Process(ctx, event)
	if err != nil {
		return fmt.Errorf("Action failed: %w", err)
	}

	err = r.To.Send(ctx, out)
	if err != nil {
		return fmt.Errorf("Destination failed: %w", err)
	}

	return nil
}

func (r *Rule[In, Out]) shouldProcess(syms boolexpr.Symbols) (bool, error) {
	if r.parsedIf == nil {
		return true, nil
	}

	shouldProcess, err := boolexpr.EvalExpression(*r.parsedIf, syms)
	if err != nil {
		return false, fmt.Errorf("Condition evaluation failed: %w", err)
	}

	return shouldProcess, nil
}

func (r *Rule[In, Out]) parseCondition() error {
	if r.If == "" {
		return nil
	}

	parsedIf, err := boolexpr.Parse(r.If)
	if err != nil {
		return err
	}

	r.parsedIf = &parsedIf

	return nil
}
