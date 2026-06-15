// Package firehose provides an event processing pipeline framework.
package firehose

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/emad-elsaid/boolexpr"
	"golang.org/x/time/rate"
)

// ErrIncompatibleSource is returned when the next rule in the same source chain doesn't have the same source type.
var ErrIncompatibleSource = errors.New("next rule doesn't have the same source")

// Rule defines an event processing pipeline from source to destination.
type Rule[I, O Event] struct {
	ID   string
	When Source[I] `validate:"required"`

	If        string
	RateLimit rate.Limit    // events per second, 0 means no rate limit
	OnceEvery time.Duration // duration to allow only one event with the same ID, 0 means no once
	CacheFor  time.Duration // duration to cache the output of the action, 0 means no caching

	Then Action[I, O]   `validate:"required"`
	To   Destination[O] `validate:"required"`

	next, prev                     Registry
	nextSameSource, prevSameSource sourceRegistry

	ctx             context.Context
	wrappedCallback Callback[I]
}

func (r *Rule[I, O]) start(ctx context.Context) error {
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

func (r *Rule[I, O]) callback(ctx context.Context, event I, reports chan<- Report) {
	attrs, err := EventAttributes(ctx, event)
	if err != nil {
		reports <- NewRuleReport(r.ID, StatusError, fmt.Errorf("failed to get event attributes: %w", err))

		return
	}

	syms := boolexpr.NewSymbolsCached(attrs)
	r.run(ctx, event, syms, reports)
}

func (r *Rule[I, O]) run(ctx context.Context, event I, syms boolexpr.Symbols, reports chan<- Report) {
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

func (r *Rule[I, O]) runCurrent(ctx context.Context, event I, syms boolexpr.Symbols, reports chan<- Report) {
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

func (r *Rule[I, O]) getNext() Registry                  { return r.next }
func (r *Rule[I, O]) setNext(n Registry)                 { r.next = n }
func (r *Rule[I, O]) getPrev() Registry                  { return r.prev }
func (r *Rule[I, O]) setPrev(p Registry)                 { r.prev = p }
func (r *Rule[I, O]) setNextSameSource(n sourceRegistry) { r.nextSameSource = n }
func (r *Rule[I, O]) getNextSameSource() sourceRegistry  { return r.nextSameSource }
func (r *Rule[I, O]) setPrevSameSource(p sourceRegistry) { r.prevSameSource = p }
func (r *Rule[I, O]) getSourceRegistry() sourceRegistry  { return r }
func (r *Rule[I, O]) getRegistry() Registry              { return r }
func (r *Rule[I, O]) getCtx() context.Context            { return r.ctx }
func (r *Rule[I, O]) getSource() any                     { return r.When }

func (r *Rule[I, O]) hasNextRunnable() bool {
	return r.nextSameSource != nil
}

func (r *Rule[I, O]) nextRunnable() (runnable[I], error) {
	runnable, ok := r.nextSameSource.getRegistry().(runnable[I])
	if !ok {
		return nil, fmt.Errorf("%w: rule %#v, next %#v", ErrIncompatibleSource, r, r.nextSameSource)
	}

	return runnable, nil
}
