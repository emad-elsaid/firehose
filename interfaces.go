package firehose

import (
	"context"

	"github.com/emad-elsaid/boolexpr"
)

// Event represents an event with attributes that can be evaluated in conditions.
type Event interface {
	Attributes(ctx context.Context) map[string]any
}

// Source produces events of type T.
type Source[T any] interface {
	Start(ctx context.Context, cb SourceCallback[T]) (done context.Context, err error)
}

// Condition evaluates input events to determine if they should be processed.
type Condition[In any] interface {
	Eval(ctx context.Context, event In) (bool, error)
}

// Action transforms input events to output events.
type Action[In, Out any] interface {
	Process(ctx context.Context, event In) (Out, error)
}

// Destination consumes events of type T.
type Destination[T any] interface {
	Send(ctx context.Context, event T) error
}

// Registry handler that accumulates rules and manages their execution.
type Registry interface {
	getNext() Registry
	setNext(n Registry)
	getPrev() Registry
	setPrev(p Registry)

	getSource() any
	getCtx() context.Context
	start(ctx context.Context) error

	getSourceRegistry() sourceRegistry
}

type sourceRegistry interface {
	setNextSameSource(n sourceRegistry)
	setPrevSameSource(p sourceRegistry)

	getRegistry() Registry
}

// SourceCallback is a function type that sources use to send events to the
// engine. It takes a context and an event of type T, and returns a channel of
// Report which the engine will write to with the results of processing the
// event through each rule.
type SourceCallback[T any] func(context.Context, T) <-chan Report

type callbackable[In any] interface {
	callbackWithSyms(ctx context.Context, event In, syms boolexpr.Symbols, reports chan<- Report)
}
