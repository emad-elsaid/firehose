// Package firehose provides an event processing pipeline framework.
package firehose

import (
	"context"
	"errors"
	"fmt"
)

type (
	// Rule defines an event processing pipeline from source to destination.
	Rule[In, Out any] struct {
		When Source[In]
		If   string
		Then Action[In, Out]
		To   Destination[Out]

		next, prev Registery
	}

	// Source produces events of type T.
	Source[T any] interface {
		ID() string
		Start(ctx context.Context, cb func(context.Context, T) error) (done context.Context, err error)
	}

	// Action transforms input events to output events.
	Action[In, Out any] interface {
		Process(ctx context.Context, event In) (Out, error)
	}

	// Destination consumes events of type T.
	Destination[T any] interface {
		Send(event T) error
	}

	Registery interface {
		Activator() activator
		GetNext() Registery
		SetNext(n Registery)
		GetPrev() Registery
		SetPrev(p Registery)
	}

	activator func(context.Context) (context.Context, error)
)

func (r *Rule[In, Out]) GetNext() Registery {
	return r.next
}

func (r *Rule[In, Out]) SetNext(n Registery) {
	r.next = n
}

func (r *Rule[In, Out]) GetPrev() Registery {
	return r.prev
}

func (r *Rule[In, Out]) SetPrev(p Registery) {
	r.prev = p
}

func (r *Rule[In, Out]) Activator() activator {
	return func(ctx context.Context) (context.Context, error) {
		return r.When.Start(ctx, ruleToCallback(r))
	}
}

// AddRule registers a new processing rule in the context.
func AddRule[In, Out any](r Registery, rule *Rule[In, Out]) (Registery, error) {
	if r == nil {
		return rule, nil
	}

	rule.SetNext(r)
	r.SetPrev(rule)

	return rule, nil
}

func ruleToCallback[In, Out any](rule *Rule[In, Out]) func(context.Context, In) error {
	return func(ctx context.Context, event In) error {
		out, err := rule.Then.Process(ctx, event)
		if err != nil {
			return fmt.Errorf("Action failed: %w", err)
		}

		err = rule.To.Send(out)
		if err != nil {
			return fmt.Errorf("Destination failed: %w", err)
		}

		return nil
	}
}

// Start activates all registered rules and waits for completion.
func Start(ctx context.Context, r Registery) error {
	contexts := make([]context.Context, 0)

	for i := r; i != nil; i = i.GetNext() {
		activator := i.Activator()
		sourceCtx, err := activator(ctx)
		if err != nil {
			return err
		}

		contexts = append(contexts, sourceCtx)
	}

	<-ctx.Done()

	return waitForSourcesToFinish(contexts)
}

func waitForSourcesToFinish(contexts []context.Context) error {
	errs := make([]error, 0, len(contexts))

	for _, ctx := range contexts {
		<-ctx.Done()

		err := ctx.Err()
		if err != nil {
			errs = append(errs, err)
		}
	}

	return errors.Join(errs...)
}
