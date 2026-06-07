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
	ID   string
	When Source[In] `validate:"required"`
	If   string
	Then Action[In, Out]  `validate:"required"`
	To   Destination[Out] `validate:"required"`

	next, prev                     Registry
	nextSameSource, prevSameSource sourceRegistry

	ctx             context.Context
	wrappedCallback SourceCallback[In]
}

func (r *Rule[In, Out]) start(ctx context.Context) error {
	isFirstSameSource := r.prevSameSource == nil
	if !isFirstSameSource {
		return nil
	}

	// use default callback function if not wrapped by any middleware,
	// otherwise use the wrapped callback.
	cb := r.callback
	if r.wrappedCallback != nil {
		cb = r.wrappedCallback
	}

	ctx, err := r.When.Start(ctx, cb)
	if err != nil {
		return fmt.Errorf("failed to start source: %w", err)
	}

	r.ctx = ctx

	return nil
}

// Must be from the source not called internally.
func (r *Rule[In, Out]) callback(ctx context.Context, event In) <-chan Report {
	reports := make(chan Report)

	go r.callbackWithChan(ctx, event, reports)

	return reports
}

func (r *Rule[In, Out]) callbackWithChan(ctx context.Context, event In, reports chan<- Report) {
	defer close(reports)

	attrs, err := event.Attributes(ctx)
	if err != nil {
		reports <- NewRuleReport(r.ID, StatusError, fmt.Errorf("failed to get event attributes: %w", err))

		return
	}

	syms := boolexpr.NewSymbolsCached(attrs)
	r.run(ctx, event, syms, reports)
}

func (r *Rule[In, Out]) run(ctx context.Context, event In, syms boolexpr.Symbols, reports chan<- Report) {
	r.runCurrent(ctx, event, syms, reports)

	if !r.hasNextRunnable() {
		return
	}

	nextRunnable, err := r.nextRunnable()
	if err != nil {
		reports <- NewRuleReport(r.ID, StatusError, err)

		return
	}

	nextRunnable.run(ctx, event, syms, reports)
}

func (r *Rule[In, Out]) runCurrent(ctx context.Context, event In, syms boolexpr.Symbols, reports chan<- Report) {
	out, report := r.Then.Process(ctx, event, syms)
	report.Rule = r.ID

	if report.Err != nil || report.Abort {
		reports <- report

		return
	}

	report = r.To.Send(ctx, out)
	report.Rule = r.ID

	reports <- report
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

func (r *Rule[In, Out]) hasNextRunnable() bool {
	return r.nextSameSource != nil
}

func (r *Rule[In, Out]) nextRunnable() (runnable[In], error) {
	runnable, ok := r.nextSameSource.getRegistry().(runnable[In])
	if !ok {
		return nil, fmt.Errorf("%w: rule %#v, next %#v", ErrIncompatibleSource, r, r.nextSameSource)
	}

	return runnable, nil
}
