package firehose

import (
	"context"

	"github.com/emad-elsaid/boolexpr"
	"github.com/mitchellh/hashstructure/v2"
)

// Event represents an event with attributes that can be evaluated in conditions.
type Event interface{}

func EventID(event any) (uint64, error) {
	return hashstructure.Hash(event, hashstructure.FormatV2, nil)
}

// Interface for providing attributes for an event
type Attributer interface {
	Attributes(ctx context.Context) (map[string]any, error)
}

func EventAttributes(ctx context.Context, event any) (map[string]any, error) {
	if attributer, ok := event.(Attributer); ok {
		return attributer.Attributes(ctx)
	}

	return nil, nil
}

// Source produces events of type T.
type Source[T any] interface {
	Start(ctx context.Context, cb Callback[T]) (done context.Context, err error)
}

// Action transforms input events to output events.
type Action[In, Out any] interface {
	Process(ctx context.Context, event In, syms boolexpr.Symbols) (Out, Report)
}

// Destination consumes events of type T.
type Destination[T any] interface {
	Send(ctx context.Context, event T) Report
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
	getNextSameSource() sourceRegistry

	getRegistry() Registry
}

// Callback is a function type that sources use to send events to the
// engine. It takes a context and an event of type T, and a channel of
// Report which the engine will write to with the results of processing the
// event through each rule.
type Callback[T any] func(context.Context, T, chan<- Report)

type runnable[In any] interface {
	run(ctx context.Context, event In, syms boolexpr.Symbols, reports chan<- Report)
}

// ActionMiddleware wraps actions to add cross-cutting concerns such as conditional execution,
// panic recovery, or logging.
type ActionMiddleware[In, Out Event] interface {
	Wrap(ctx context.Context, rule Rule[In, Out], action Action[In, Out], in In) (Action[In, Out], error)
}

// DestinationMiddleware wraps destinations to add cross-cutting concerns such as panic recovery,
// retry logic, or telemetry.
type DestinationMiddleware[In, Out Event] interface {
	Wrap(ctx context.Context, rule Rule[In, Out], destination Destination[Out], out Out) (Destination[Out], error)
}

// CallbackMiddleware wraps source callbacks to add cross-cutting concerns such as conditional execution.
type CallbackMiddleware[In, Out Event] interface {
	Wrap(ctx context.Context, rule Rule[In, Out], callback Callback[In], in In) (Callback[In], error)
}
