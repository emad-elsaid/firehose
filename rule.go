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

		next, prev Registry
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

	// Registry handler that accumulates rules and manages their execution.
	Registry interface {
		activator() activator
		getNext() Registry
		setNext(n Registry)
		getPrev() Registry
		setPrev(p Registry)
	}

	activator func(context.Context) (context.Context, error)
)

func (r *Rule[In, Out]) getNext() Registry {
	return r.next
}

func (r *Rule[In, Out]) setNext(n Registry) {
	r.next = n
}

func (r *Rule[In, Out]) getPrev() Registry {
	return r.prev
}

func (r *Rule[In, Out]) setPrev(p Registry) {
	r.prev = p
}

func (r *Rule[In, Out]) activator() activator {
	return func(ctx context.Context) (context.Context, error) {
		return r.When.Start(ctx, ruleToCallback(r))
	}
}

// AddRule registers a new processing rule in the context.
func AddRule[In, Out any](registry Registry, rule *Rule[In, Out]) (Registry, error) {
	if registry == nil {
		return rule, nil
	}

	rule.setNext(registry)
	registry.setPrev(rule)

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
func Start(ctx context.Context, r Registry) error {
	contexts := make([]context.Context, 0)

	for i := r; i != nil; i = i.getNext() {
		activator := i.activator()

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
