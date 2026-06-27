// Package firehose provides an event processing pipeline framework.
package firehose

import (
	"context"
	"fmt"

	"github.com/emad-elsaid/boolexpr"
)

// Rule defines an event processing pipeline from source to destination.
type Rule[I, O Event] struct {
	// ID is a unique identifier for the rule, used for reporting and debugging purposes.
	ID string
	// On is the source that produces events to be processed by this rule.
	On Source[I] `validate:"required_without=SubRules"`
	// If conditions are evaluated in sequence. If any condition returns false or an error,
	// the rule processing is aborted. Conditions are evaluated by value, allowing you
	// to use types like ifs.Cond (string expressions), ifs.RateLimit, and ifs.Once.
	If []If[I]
	// Then is the action to process the event if the On source produces an event
	Then Action[I, O] `validate:"required_without=SubRules"`
	// To is the destination to send the output of the Then action
	To Destination[O] `validate:"required_without=SubRules"`
	// SubRules are the child rules that will inherit the parent fields if set
	SubRules []Rule[I, O]

	next, prev                     Registry
	nextSameSource, prevSameSource sourceRegistry

	ctx                 context.Context
	wrappedCallback     Callback[I]
	actionWrappers      Action[I, O]
	destinationWrappers Destination[O]
}

// Process implements the Action interface. it allows using the rule as an action during the wrapping
// of the action. so that when the action field changes it called the new action.
// When called it calls the current action without any middlewares.
func (r *Rule[I, O]) Process(ctx context.Context, event I, syms boolexpr.Symbols) (O, Report) {
	return r.Then.Process(ctx, event, syms)
}

// Send implements the Destination interface. it allows using the rule as a
// destination during the wrapping of the destination. so that when the
// destination field changes it called the new destination.
func (r *Rule[I, O]) Send(ctx context.Context, event O) Report {
	return r.To.Send(ctx, event)
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

	srcCtx, err := r.On.Start(ctx, cb)
	if err != nil {
		return fmt.Errorf("failed to start source: %w", err)
	}

	r.ctx = srcCtx

	return nil
}

func (r *Rule[I, O]) callback(ctx context.Context, event I, reports chan<- Report) {
	attrs, err := EventAttributes(ctx, event)
	if err != nil {
		reports <- NewRuleReport(r.ID, StatusError, fmt.Errorf("failed to get event attributes: %w", err))

		return
	}

	syms := boolexpr.NewSymbolsCached(attrs)

	for current := Runnable[I](r); current != nil; current = current.NextRunnable() {
		current.Run(ctx, event, syms, reports)
	}
}

// Run executes the rule's action and destination for the given event.
func (r *Rule[I, O]) Run(ctx context.Context, event I, syms boolexpr.Symbols, reports chan<- Report) {
	// Evaluate conditions first
	for _, cond := range r.If {
		pass, err := cond.Evaluate(ctx, event, syms)
		if err != nil {
			reports <- NewRuleReport(r.ID, StatusError, fmt.Errorf("condition error: %w", err))
			return
		}

		if !pass {
			reports <- NewRuleReport(r.ID, StatusNoMatch, nil)
			return
		}
	}

	// Use wrapped action if available, otherwise use the direct action
	action := r.Then
	if r.actionWrappers != nil {
		action = r.actionWrappers
	}
	out, report := action.Process(ctx, event, syms)
	report.Rule = r.ID

	if report.Err != nil || report.Abort {
		reports <- report

		return
	}

	// Use wrapped destination if available, otherwise use the direct destination
	destination := r.To
	if r.destinationWrappers != nil {
		destination = r.destinationWrappers
	}
	report = destination.Send(ctx, out)
	report.Rule = r.ID

	reports <- report
}

// NextRunnable returns the next runnable rule with the same source.
func (r *Rule[I, O]) NextRunnable() Runnable[I] {
	if r.nextSameSource == nil {
		return nil
	}

	// We will panic on purpose in case the next source is not a Runnable of the same type
	// As this would indicate a bug in the engine.
	//nolint:forcetypeassert // Intentional panic on type mismatch
	return r.nextSameSource.getRegistry().(Runnable[I])
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
func (r *Rule[I, O]) getSource() any                     { return r.On }
