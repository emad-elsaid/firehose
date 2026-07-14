// Package firehose provides an event processing pipeline framework.
package firehose

import (
	"context"
	"errors"
	"fmt"

	"github.com/emad-elsaid/boolexpr"
)

// Rule defines an event processing pipeline from source to destination.
// I and O represent the input and output event types.
type Rule[I, O any] struct {
	// ID is a unique identifier for the rule, used for reporting and debugging purposes.
	ID string `validate:"required"`
	// Environments is a list of environment names where the rule is active. If
	// empty, the rule is active in all environments.
	Environments []string
	// Select is the action to process input events into output events.
	Select Action[I, O] `validate:"required"`
	// Into is the destination to send the output of the Select action.
	Into Destination[O] `validate:"required"`
	// From is the source that produces events to be processed by this rule.
	From Source[I] `validate:"required"`
	// Where is a condition that must evaluate to true for the rule to process the event.
	// Use condition.Cond for string expressions, condition.RateLimit for rate limiting,
	// condition.Once for deduplication, or condition.Conditions for combining multiple conditions.
	Where Condition[I]
	// Having is a condition that must evaluate to true for the rule to send
	// the output of the Select action to the Into destination.
	Having Condition[O]
	// Middlewares are the middlewares that will be applied to the action and
	// destination and callback of the rule. The first middleware wraps the
	// second middleware, and so on. The last middleware wraps the
	// actions/destination/callback of the rule.
	Middlewares []Middleware[I, O]

	next, prev                     Registry
	nextSameSource, prevSameSource sourceRegistry

	done               <-chan struct{}
	wrappedCallback    Callback[I]
	wrappedAction      Action[I, O]
	wrappedDestination Destination[O]
}

func (r *Rule[I, O]) start(ctx context.Context) error {
	isFirstSameSource := r.prevSameSource == nil
	if !isFirstSameSource {
		return nil
	}

	done, err := r.From.Start(ctx, r.wrappedCallback)
	if err != nil {
		return fmt.Errorf("failed to start source: %w", err)
	}

	r.done = done

	return nil
}

func (r *Rule[I, O]) callback(ctx context.Context, event I, reportFn ErrorHandler) {
	syms := EventSymbols(event)

	for current := Runnable[I](r); current != nil; current = current.NextRunnable() {
		err := current.Run(ctx, event, syms)
		if err != nil && reportFn != nil {
			reportFn(err)
		}
	}
}

// Run executes the rule's action and destination for the given event.
func (r *Rule[I, O]) Run(ctx context.Context, event I, syms boolexpr.Symbols) error {
	// Evaluate input condition
	if r.Where != nil {
		pass, err := r.Where.Evaluate(ctx, event, syms)
		if err != nil {
			return NewRuleError(r.ID, ConditionError{Err: err})
		}

		if !pass {
			return NewRuleError(r.ID, ErrInputNoMatch)
		}
	}

	// Process action
	output, err := r.wrappedAction.Process(ctx, event, syms)
	if err != nil {
		var actionErr ActionError
		if !errors.As(err, &actionErr) {
			err = ActionError{Err: err}
		}
		return NewRuleError(r.ID, err)
	}

	// Evaluate output condition
	if r.Having != nil {
		outputSyms := EventSymbols(output)
		pass, err := r.Having.Evaluate(ctx, output, outputSyms)
		if err != nil {
			return NewRuleError(r.ID, ConditionError{Err: err})
		}

		if !pass {
			return NewRuleError(r.ID, ErrOutputNoMatch)
		}
	}

	// Send to destination
	err = r.wrappedDestination.Send(ctx, output)
	if err != nil {
		var destinationErr DestinationError
		if !errors.As(err, &destinationErr) {
			err = DestinationError{Err: err}
		}
		return NewRuleError(r.ID, err)
	}

	return nil
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
func (r *Rule[I, O]) getDone() <-chan struct{}           { return r.done }
func (r *Rule[I, O]) getSource() any                     { return r.From }
