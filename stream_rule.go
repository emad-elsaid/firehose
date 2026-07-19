//nolint:dupl // Intentionally mirrors SQLRule/ScenarioRule with Kafka Streams field names
package firehose

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/emad-elsaid/boolexpr"
)

// StreamRule defines an event processing pipeline following the Kafka Streams
// pattern. I and O represent the input and output event types.
//
// Pipeline: Source → Filter → Map → FilterOutput → Sink.
type StreamRule[I, O any] struct {
	// ID is a unique identifier for the rule, used for reporting and
	// debugging purposes.
	ID string `validate:"required"`
	// Environments is a list of environment names where the rule is active.
	// If empty, the rule is active in all environments.
	Environments []string
	// Source is the source that produces events to be processed by this rule.
	Source Source[I] `validate:"required"`
	// Filter is a condition that must evaluate to true for the rule to process
	// the event.
	Filter Condition[I]
	// Map is the action to process input events into output events.
	Map Action[I, O] `validate:"required"`
	// FilterOutput is a condition that must evaluate to true for the rule to
	// send the output of the Map action to the Sink destination.
	FilterOutput Condition[O]
	// Sink is the destination to send the output of the Map action.
	Sink Destination[O] `validate:"required"`
	// Middlewares are the middlewares that will be applied to the action and
	// destination and callback of the rule. The first middleware wraps the
	// second middleware, and so on. The last middleware wraps the
	// actions/destination/callback of the rule.
	Middlewares []Middleware[I, O]

	next, prev                     Rule
	nextSameSource, prevSameSource Rule

	wrappedCallback Callback[I]
}

// Init initializes the rule by wrapping the callback, action, and destination
// with the configured middlewares. Middlewares are applied in reverse order,
// so the first middleware in the slice wraps all subsequent ones.
func (r *StreamRule[I, O]) Init(ctx context.Context) error {
	r.wrappedCallback = r.callback

	if len(r.Middlewares) == 0 {
		return nil
	}

	for i := range slices.Backward(r.Middlewares) {
		middleware := r.Middlewares[i]

		wrappedCallback, err := middleware.WrapCallback(ctx, r, r.wrappedCallback)
		if err != nil {
			return err
		}
		r.wrappedCallback = wrappedCallback

		wrappedAction, err := middleware.WrapAction(ctx, r, r.Map)
		if err != nil {
			return err
		}
		r.Map = wrappedAction

		wrappedDestination, err := middleware.WrapDestination(ctx, r, r.Sink)
		if err != nil {
			return err
		}
		r.Sink = wrappedDestination
	}

	return nil
}

// Start begins the source for this rule if it's the first rule with
// this source in the chain. Returns a done channel when the source stops.
//
//nolint:nilnil // Intentional: returning nil,nil means "nothing to start"
func (r *StreamRule[I, O]) Start(ctx context.Context) (<-chan struct{}, error) {
	isFirstSameSource := r.prevSameSource == nil
	if !isFirstSameSource {
		return nil, nil
	}

	done, err := r.Source.Start(ctx, r.wrappedCallback)
	if err != nil {
		return nil, fmt.Errorf("failed to start source: %w", err)
	}

	return done, nil
}

func (r *StreamRule[I, O]) callback(ctx context.Context, event I, reportFn ErrorHandler) {
	syms := EventSymbols(event)

	for current := Runnable[I](r); current != nil; current = current.NextRunnable() {
		err := current.Run(ctx, event, syms)
		if err != nil && reportFn != nil {
			reportFn(err)
		}
	}
}

// Run executes the rule's action and destination for the given event.
//
//nolint:dupl // Intentionally mirrors SQLRule.Run with Kafka Streams field names
func (r *StreamRule[I, O]) Run(ctx context.Context, event I, syms boolexpr.Symbols) error {
	if r.Filter != nil {
		pass, err := r.Filter.Evaluate(ctx, event, syms)
		if err != nil {
			return NewRuleError(r.ID, ConditionError{Err: err})
		}

		if !pass {
			return NewRuleError(r.ID, ErrInputNoMatch)
		}
	}

	output, err := r.Map.Process(ctx, event, syms)
	if err != nil {
		var actionErr ActionError
		if !errors.As(err, &actionErr) {
			err = ActionError{Err: err}
		}

		return NewRuleError(r.ID, err)
	}

	if r.FilterOutput != nil {
		outputSyms := EventSymbols(output)

		pass, err := r.FilterOutput.Evaluate(ctx, output, outputSyms)
		if err != nil {
			return NewRuleError(r.ID, ConditionError{Err: err})
		}

		if !pass {
			return NewRuleError(r.ID, ErrOutputNoMatch)
		}
	}

	err = r.Sink.Send(ctx, output)
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
func (r *StreamRule[I, O]) NextRunnable() Runnable[I] {
	if r.nextSameSource == nil {
		return nil
	}

	//nolint:forcetypeassert // Intentional panic on type mismatch
	return r.nextSameSource.(Runnable[I])
}

// GetNext returns the next rule in the circular rule list.
func (r *StreamRule[I, O]) GetNext() Rule { return r.next }

// SetNext sets the next rule in the circular rule list.
func (r *StreamRule[I, O]) SetNext(n Rule) { r.next = n }

// GetPrev returns the previous rule in the circular rule list.
func (r *StreamRule[I, O]) GetPrev() Rule { return r.prev }

// SetPrev sets the previous rule in the circular rule list.
func (r *StreamRule[I, O]) SetPrev(p Rule) { r.prev = p }

// SetNextSameSource sets the next rule sharing the same source.
func (r *StreamRule[I, O]) SetNextSameSource(n Rule) { r.nextSameSource = n }

// GetNextSameSource returns the next rule sharing the same source.
func (r *StreamRule[I, O]) GetNextSameSource() Rule { return r.nextSameSource }

// SetPrevSameSource sets the previous rule sharing the same source.
func (r *StreamRule[I, O]) SetPrevSameSource(p Rule) { r.prevSameSource = p }

// GetPrevSameSource returns the previous rule sharing the same source.
func (r *StreamRule[I, O]) GetPrevSameSource() Rule { return r.prevSameSource }

// GetSource returns the source associated with this rule.
func (r *StreamRule[I, O]) GetSource() any { return r.Source }

// GetID returns the unique identifier of the rule.
func (r *StreamRule[I, O]) GetID() string { return r.ID }

// GetEnvironments returns the list of environments where the rule is active.
func (r *StreamRule[I, O]) GetEnvironments() []string { return r.Environments }
