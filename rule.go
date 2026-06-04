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

// Must be from the source not called internally
func (r *Rule[In, Out]) callback(ctx context.Context, event In) <-chan Report {
	reports := make(chan Report, 0)

	go func() {
		r.callbackWithSyms(ctx, event, nil, reports)
		close(reports)
	}()

	return reports
}

func (r *Rule[In, Out]) callbackWithSyms(ctx context.Context, event In, syms boolexpr.Symbols, reports chan<- Report) {
	if r.parsedIf != nil && syms == nil {
		syms = boolexpr.NewSymbolsCached(event.Attributes(ctx))
	}

	r.run(ctx, event, syms, reports)
	r.callNextRule(ctx, event, syms, reports)
}

func (r *Rule[In, Out]) callNextRule(ctx context.Context, event In, syms boolexpr.Symbols, reports chan<- Report) {
	if r.nextSameSource == nil {
		return
	}

	callbackable, ok := r.nextSameSource.getRegistry().(callbackable[In])
	if !ok {
		reports <- NewReport(StatusError, fmt.Errorf("%w: current rule %#v, next %#v", ErrIncompatibleSource, r, r.nextSameSource))

		return
	}

	callbackable.callbackWithSyms(ctx, event, syms, reports)
}

func (r *Rule[In, Out]) run(ctx context.Context, event In, syms boolexpr.Symbols, reports chan<- Report) {
	shouldProcess, err := r.shouldProcess(syms)
	if err != nil {
		reports <- NewReport(StatusConditionError, err)

		return
	}

	if !shouldProcess {
		reports <- NewReport(StatusNoMatch, nil)

		return
	}

	out, err := r.Then.Process(ctx, event)
	if err != nil {
		reports <- NewReport(StatusActionError, err)

		return
	}

	err = r.To.Send(ctx, out)
	if err != nil {
		reports <- NewReport(StatusDestinationError, err)

		return
	}

	reports <- NewReport(StatusSuccess, nil)
}

func (r *Rule[In, Out]) shouldProcess(syms boolexpr.Symbols) (bool, error) {
	if r.parsedIf == nil {
		return true, nil
	}

	shouldProcess, err := boolexpr.EvalExpression(*r.parsedIf, syms)
	if err != nil {
		return false, err
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
