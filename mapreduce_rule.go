package firehose

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"

	"github.com/emad-elsaid/boolexpr"
)

// MapReduceRule defines an event processing pipeline following the MapReduce
// pattern. I is the input event type, M is the intermediary type produced by
// Map, and Out is the accumulated output type.
//
// Pipeline: Source → Filter → Map (I → M) → Reduce (M + Out → Out) →
//
//	FilterOutput → Sink.
type MapReduceRule[I, M, Out any] struct {
	// ID is a unique identifier for the rule.
	ID string `validate:"required"`
	// Environments is a list of environment names where the rule is active.
	Environments []string
	// Source is the source that produces events to be processed by this rule.
	Source Source[I] `validate:"required"`
	// Filter is a condition that must evaluate to true for the rule to process
	// the event.
	Filter Condition[I]
	// Map transforms input events into intermediary values.
	Map Action[I, M] `validate:"required"`
	// Reduce combines an intermediary value with the current accumulator to
	// produce a new output value.
	Reduce Reducer[M, Out] `validate:"required"`
	// FilterOutput is a condition that must evaluate to true for the rule to
	// send the reduced output to the Sink destination.
	FilterOutput Condition[Out]
	// Sink is the destination that consumes the reduced output.
	Sink Destination[Out] `validate:"required"`
	// Middlewares are applied to the callback and Map action. Middlewares wrap
	// in reverse order (first middleware wraps last). Note: middlewares only
	// wrap the callback and Map action — the Sink destination type (Out)
	// differs from the intermediary type (M), so destination wrapping is not
	// supported.
	Middlewares []Middleware[I, M]

	next, prev                     Rule
	nextSameSource, prevSameSource Rule

	wrappedCallback Callback[I]
	mu              sync.Mutex
	accum           Out
}

// Init initializes the rule by wrapping the callback and Map action with the
// configured middlewares. Middlewares are applied in reverse order.
func (r *MapReduceRule[I, M, Out]) Init(ctx context.Context) error {
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
	}

	return nil
}

// Start begins the source for this rule if it's the first rule with this
// source in the chain.
//
//nolint:nilnil // Intentional: returning nil,nil means "nothing to start"
func (r *MapReduceRule[I, M, Out]) Start(ctx context.Context) (<-chan struct{}, error) {
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

func (r *MapReduceRule[I, M, Out]) callback(ctx context.Context, event I, reportFn ErrorHandler) {
	syms := EventSymbols(event)

	for current := Runnable[I](r); current != nil; current = current.NextRunnable() {
		err := current.Run(ctx, event, syms)
		if err != nil && reportFn != nil {
			reportFn(err)
		}
	}
}

// Run maps the input event to an intermediary value, reduces it with the
// current accumulator, then sends the result to the sink.
func (r *MapReduceRule[I, M, Out]) Run(ctx context.Context, event I, syms boolexpr.Symbols) error {
	if r.Filter != nil {
		pass, err := r.Filter.Evaluate(ctx, event, syms)
		if err != nil {
			return NewRuleError(r.ID, ConditionError{Err: err})
		}

		if !pass {
			return NewRuleError(r.ID, ErrInputNoMatch)
		}
	}

	intermediary, err := r.Map.Process(ctx, event, syms)
	if err != nil {
		return NewRuleError(r.ID, ActionError{Err: err})
	}

	r.mu.Lock()

	newAccum, err := r.Reduce.Reduce(ctx, intermediary, r.accum)
	if err != nil {
		r.mu.Unlock()

		return NewRuleError(r.ID, ReduceError{Err: err})
	}

	r.accum = newAccum
	r.mu.Unlock()

	if r.FilterOutput != nil {
		outputSyms := EventSymbols(newAccum)

		pass, err := r.FilterOutput.Evaluate(ctx, newAccum, outputSyms)
		if err != nil {
			return NewRuleError(r.ID, ConditionError{Err: err})
		}

		if !pass {
			return NewRuleError(r.ID, ErrOutputNoMatch)
		}
	}

	err = r.Sink.Send(ctx, newAccum)
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
func (r *MapReduceRule[I, M, Out]) NextRunnable() Runnable[I] {
	if r.nextSameSource == nil {
		return nil
	}

	//nolint:forcetypeassert // Intentional panic on type mismatch
	return r.nextSameSource.(Runnable[I])
}

// GetNext returns the next rule in the circular rule list.
func (r *MapReduceRule[I, M, Out]) GetNext() Rule { return r.next }

// SetNext sets the next rule in the circular rule list.
func (r *MapReduceRule[I, M, Out]) SetNext(n Rule) { r.next = n }

// GetPrev returns the previous rule in the circular rule list.
func (r *MapReduceRule[I, M, Out]) GetPrev() Rule { return r.prev }

// SetPrev sets the previous rule in the circular rule list.
func (r *MapReduceRule[I, M, Out]) SetPrev(p Rule) { r.prev = p }

// SetNextSameSource sets the next rule sharing the same source.
func (r *MapReduceRule[I, M, Out]) SetNextSameSource(n Rule) { r.nextSameSource = n }

// GetNextSameSource returns the next rule sharing the same source.
func (r *MapReduceRule[I, M, Out]) GetNextSameSource() Rule { return r.nextSameSource }

// SetPrevSameSource sets the previous rule sharing the same source.
func (r *MapReduceRule[I, M, Out]) SetPrevSameSource(p Rule) { r.prevSameSource = p }

// GetPrevSameSource returns the previous rule sharing the same source.
func (r *MapReduceRule[I, M, Out]) GetPrevSameSource() Rule { return r.prevSameSource }

// GetSource returns the source associated with this rule.
func (r *MapReduceRule[I, M, Out]) GetSource() any { return r.Source }

// GetID returns the unique identifier of the rule.
func (r *MapReduceRule[I, M, Out]) GetID() string { return r.ID }

// GetEnvironments returns the list of environments where the rule is active.
func (r *MapReduceRule[I, M, Out]) GetEnvironments() []string { return r.Environments }
