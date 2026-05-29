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
		nextSameSource, prevSameSource sourceRegistry

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
		getPrev() Registry
		setPrev(p Registry)

		getSource() any
		getCtx() context.Context
		start(ctx context.Context) error

		getSourceRegistry() sourceRegistry
	}

	sourceRegistry interface {
		setNextSameSource(n sourceRegistry)
		setPrevSameSource(p sourceRegistry)

		getRegistry() Registry
	}

	callbackable[In any] interface {
		callback(ctx context.Context, event In) error
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

func (r *Rule[In, Out]) setNextSameSource(n sourceRegistry) {
	r.nextSameSource = n
}

func (r *Rule[In, Out]) setPrevSameSource(p sourceRegistry) {
	r.prevSameSource = p
}

func (r *Rule[In, Out]) getSourceRegistry() sourceRegistry {
	return r
}

func (r *Rule[In, Out]) getRegistry() Registry {
	return r
}

func (r *Rule[In, Out]) start(ctx context.Context) error {
	isFirstSameSource := r.prevSameSource == nil
	if !isFirstSameSource {
		return nil
	}

	sourceCtx, err := r.When.Start(ctx, r.callback)
	if err != nil {
		return err
	}

	r.ctx = sourceCtx

	return nil
}

func (r *Rule[In, Out]) callback(ctx context.Context, event In) error {
	out, err := r.Then.Process(ctx, event)
	if err != nil {
		return fmt.Errorf("Action failed: %w", err)
	}

	err = r.To.Send(out)
	if err != nil {
		return fmt.Errorf("Destination failed: %w", err)
	}

	if r.nextSameSource == nil {
		return nil
	}

	if callbackable, ok := r.nextSameSource.getRegistry().(callbackable[In]); ok {
		return callbackable.callback(ctx, event)
	}

	return nil
}

func (r *Rule[In, Out]) getCtx() context.Context {
	return r.ctx
}

func (r *Rule[In, Out]) getSource() any {
	return r.When
}
