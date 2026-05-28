// Package firehose provides an event processing pipeline framework.
package firehose

import (
	"context"
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
		ctx        context.Context
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
		getNext() Registry
		setNext(n Registry)
		setPrev(p Registry)
		getCtx() context.Context
		start(ctx context.Context) error
	}
)

func (r *Rule[In, Out]) getNext() Registry {
	return r.next
}

func (r *Rule[In, Out]) setNext(n Registry) {
	r.next = n
}

func (r *Rule[In, Out]) setPrev(p Registry) {
	r.prev = p
}

func (r *Rule[In, Out]) start(ctx context.Context) error {
	sourceCtx, err := r.When.Start(ctx, ruleToCallback(r))
	if err != nil {
		return err
	}

	r.ctx = sourceCtx

	return nil
}

func (r *Rule[In, Out]) getCtx() context.Context {
	return r.ctx
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
func Start(ctx context.Context, registry Registry) <-chan error {
	for i := registry; i != nil; i = i.getNext() {
		err := i.start(ctx)
		if err != nil {
			return chan1(err)
		}
	}

	<-ctx.Done()

	return waitForSourcesToFinish(registry)
}

func waitForSourcesToFinish(registry Registry) <-chan error {
	errs := make(chan error)

	go collectErrors(registry, errs)

	return errs
}

func collectErrors(r Registry, errs chan<- error) {
	for i := r; i != nil; i = i.getNext() {
		ctx := i.getCtx()

		if ctx == nil {
			continue
		}

		<-ctx.Done()

		err := ctx.Err()
		if err != nil {
			errs <- err
		}
	}

	close(errs)
}

func chan1[T any](v T) <-chan T {
	ch := make(chan T, 1)
	ch <- v

	close(ch)

	return ch
}
