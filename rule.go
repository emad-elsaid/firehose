// Package firehose provides an event processing pipeline framework.
package firehose

import (
	"context"
	"fmt"

	"github.com/emad-elsaid/boolexpr"
)

type (
	// Rule defines an event processing pipeline from source to destination.
	Rule[In, Out Event] struct {
		When Source[In]
		If   string
		Then Action[In, Out]
		To   Destination[Out]

		next, prev                     Registry
		nextSameSource, prevSameSource sourceRegistry

		ctx      context.Context
		parsedIf *boolexpr.Expression
	}

	Event interface {
		Attributes(ctx context.Context) map[string]any
	}

	// Source produces events of type T.
	Source[T any] interface {
		ID() string
		Start(ctx context.Context, cb func(context.Context, T) error) (done context.Context, err error)
	}

	// Condition evaluates input events to determine if they should be processed.
	Condition[In any] interface {
		Eval(ctx context.Context, event In) (bool, error)
	}

	// Action transforms input events to output events.
	Action[In, Out any] interface {
		Process(ctx context.Context, event In) (Out, error)
	}

	// Destination consumes events of type T.
	Destination[T any] interface {
		Send(event T) error
	}

	// Registry handler that accumulates rules and manages their execution.
	Registry interface {
		getNext() Registry
		setNext(n Registry)
		getPrev() Registry
		setPrev(p Registry)

		getSource() any
		getCtx() context.Context
		start(ctx context.Context) error

		getSourceRegistry() sourceRegistry
	}

	sourceRegistry interface {
		setNextSameSource(n sourceRegistry)
		setPrevSameSource(p sourceRegistry)

		getRegistry() Registry
	}

	callbackable[In any] interface {
		callbackWithSyms(ctx context.Context, event In, syms boolexpr.Symbols) error
	}
)

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

	if r.nextSameSource == nil {
		return nil
	}

	callbackable, ok := r.nextSameSource.getRegistry().(callbackable[In])
	if !ok {
		return fmt.Errorf("next rule for rule %#v is %#v doesn't have the same source", r, r.nextSameSource)
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

	err = r.To.Send(out)
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
