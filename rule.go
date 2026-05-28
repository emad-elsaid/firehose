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

		next, prev                     Registry
		nextSameSource, prevSameSource Registry

		ctx context.Context
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
		getPrev() Registry

		getCtx() context.Context
		start(ctx context.Context) error

		getSourceRegistry() sourceRegistry
	}

	sourceRegistry interface {
		getNextSameSource() Registry
		setNextSameSource(n Registry)
		getPrevSameSource() Registry
		setPrevSameSource(p Registry)
	}
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

func (r *Rule[In, Out]) getNextSameSource() Registry {
	return r.nextSameSource
}

func (r *Rule[In, Out]) setNextSameSource(n Registry) {
	r.nextSameSource = n
}

func (r *Rule[In, Out]) getPrevSameSource() Registry {
	return r.prevSameSource
}

func (r *Rule[In, Out]) setPrevSameSource(p Registry) {
	r.prevSameSource = p
}

func (r *Rule[In, Out]) getSourceRegistry() sourceRegistry {
	return r
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
	head := registry

	if head == nil {
		rule.setNext(rule)
		rule.setPrev(rule)

		return rule, nil
	}

	tail := head.getPrev()

	rule.setNext(head)
	head.setPrev(rule)

	rule.setPrev(tail)
	tail.setNext(rule)

	return head, nil
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
	for current := registry; current != nil; {
		err := current.start(ctx)
		if err != nil {
			return chan1(err)
		}

		current = current.getNext()
		if current == registry {
			break
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

func collectErrors(registry Registry, errs chan<- error) {
	for current := registry; current != nil; {
		ctx := current.getCtx()

		if ctx != nil {
			<-ctx.Done()

			err := ctx.Err()
			if err != nil {
				errs <- err
			}
		}

		current = current.getNext()
		if current == registry {
			break
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
