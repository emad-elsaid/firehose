//nolint:dupl // Intentionally mirrors SQLRule/Stream with BDD field names
package firehose

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/emad-elsaid/boolexpr"
)

// ScenarioRule defines an event processing pipeline following the BDD
// Given-When-Then pattern. I and O represent the input and output event types.
//
// Pipeline: Give (source) → Given (condition) → Then (action) →
//
//	GivenOutput (condition) → To (destination).
type ScenarioRule[I, O any] struct {
	// ID is a unique identifier for the rule, used for reporting and
	// debugging purposes.
	ID string `validate:"required"`
	// Environments is a list of environment names where the rule is active.
	// If empty, the rule is active in all environments.
	Environments []string
	// Give is the source that produces events to be processed by this rule.
	Give Source[I] `validate:"required"`
	// Given is a condition that must evaluate to true for the rule to process
	// the event.
	Given Condition[I]
	// Then is the action to process input events into output events.
	Then Action[I, O] `validate:"required"`
	// GivenOutput is a condition that must evaluate to true for the rule to
	// send the output of the Then action to the To destination.
	GivenOutput Condition[O]
	// To is the destination to send the output of the Then action.
	To Destination[O] `validate:"required"`
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
func (r *ScenarioRule[I, O]) Init(ctx context.Context) error {
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

		wrappedAction, err := middleware.WrapAction(ctx, r, r.Then)
		if err != nil {
			return err
		}
		r.Then = wrappedAction

		wrappedDestination, err := middleware.WrapDestination(ctx, r, r.To)
		if err != nil {
			return err
		}
		r.To = wrappedDestination
	}

	return nil
}

// Start begins the source for this rule if it's the first rule with
// this source in the chain. Returns a done channel when the source stops.
//
//nolint:nilnil // Intentional: returning nil,nil means "nothing to start"
func (r *ScenarioRule[I, O]) Start(ctx context.Context) (<-chan struct{}, error) {
	isFirstSameSource := r.prevSameSource == nil
	if !isFirstSameSource {
		return nil, nil
	}

	done, err := r.Give.Start(ctx, r.wrappedCallback)
	if err != nil {
		return nil, fmt.Errorf("failed to start source: %w", err)
	}

	return done, nil
}

func (r *ScenarioRule[I, O]) callback(ctx context.Context, event I, reportFn ErrorHandler) {
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
//nolint:dupl // Intentionally mirrors SQLRule.Run with BDD field names
func (r *ScenarioRule[I, O]) Run(ctx context.Context, event I, syms boolexpr.Symbols) error {
	if r.Given != nil {
		pass, err := r.Given.Evaluate(ctx, event, syms)
		if err != nil {
			return NewRuleError(r.ID, ConditionError{Err: err})
		}

		if !pass {
			return NewRuleError(r.ID, ErrInputNoMatch)
		}
	}

	output, err := r.Then.Process(ctx, event, syms)
	if err != nil {
		var actionErr ActionError
		if !errors.As(err, &actionErr) {
			err = ActionError{Err: err}
		}

		return NewRuleError(r.ID, err)
	}

	if r.GivenOutput != nil {
		outputSyms := EventSymbols(output)

		pass, err := r.GivenOutput.Evaluate(ctx, output, outputSyms)
		if err != nil {
			return NewRuleError(r.ID, ConditionError{Err: err})
		}

		if !pass {
			return NewRuleError(r.ID, ErrOutputNoMatch)
		}
	}

	err = r.To.Send(ctx, output)
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
func (r *ScenarioRule[I, O]) NextRunnable() Runnable[I] {
	if r.nextSameSource == nil {
		return nil
	}

	//nolint:forcetypeassert // Intentional panic on type mismatch
	return r.nextSameSource.(Runnable[I])
}

// GetNext returns the next rule in the circular rule list.
func (r *ScenarioRule[I, O]) GetNext() Rule { return r.next }

// SetNext sets the next rule in the circular rule list.
func (r *ScenarioRule[I, O]) SetNext(n Rule) { r.next = n }

// GetPrev returns the previous rule in the circular rule list.
func (r *ScenarioRule[I, O]) GetPrev() Rule { return r.prev }

// SetPrev sets the previous rule in the circular rule list.
func (r *ScenarioRule[I, O]) SetPrev(p Rule) { r.prev = p }

// SetNextSameSource sets the next rule sharing the same source.
func (r *ScenarioRule[I, O]) SetNextSameSource(n Rule) { r.nextSameSource = n }

// GetNextSameSource returns the next rule sharing the same source.
func (r *ScenarioRule[I, O]) GetNextSameSource() Rule { return r.nextSameSource }

// SetPrevSameSource sets the previous rule sharing the same source.
func (r *ScenarioRule[I, O]) SetPrevSameSource(p Rule) { r.prevSameSource = p }

// GetPrevSameSource returns the previous rule sharing the same source.
func (r *ScenarioRule[I, O]) GetPrevSameSource() Rule { return r.prevSameSource }

// GetSource returns the source associated with this rule.
func (r *ScenarioRule[I, O]) GetSource() any { return r.Give }

// GetID returns the unique identifier of the rule.
func (r *ScenarioRule[I, O]) GetID() string { return r.ID }

// GetEnvironments returns the list of environments where the rule is active.
func (r *ScenarioRule[I, O]) GetEnvironments() []string { return r.Environments }
