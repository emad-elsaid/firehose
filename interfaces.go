package firehose

import (
	"context"

	"github.com/emad-elsaid/boolexpr"
	"github.com/mitchellh/hashstructure/v2"
)

// Event represents an event with attributes that can be evaluated in conditions.
type Event any

// EventID computes a hash-based identifier for an event.
func EventID(event Event) (uint64, error) {
	return hashstructure.Hash(event, hashstructure.FormatV2, nil)
}

// Attributer is an interface for providing attributes for an event that can be used in condition evaluation.
type Attributer interface {
	Attributes(ctx context.Context) (map[string]any, error)
}

// EventAttributes extracts attributes from an event if it implements the Attributer interface.
func EventAttributes(ctx context.Context, event any) (map[string]any, error) {
	if attributer, ok := event.(Attributer); ok {
		return attributer.Attributes(ctx)
	}

	return map[string]any{}, nil
}

// Source produces events of type T.
type Source[T any] interface {
	Start(ctx context.Context, cb Callback[T]) (done context.Context, err error)
}

// Action transforms input events to output events.
type Action[I, O any] interface {
	Process(ctx context.Context, event I, syms boolexpr.Symbols) (O, Report)
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
type Callback[I any] func(context.Context, I, chan<- Report)

// Runnable represents a rule that can be executed to process events.
type Runnable[I any] interface {
	Run(ctx context.Context, event I, syms boolexpr.Symbols, reports chan<- Report)
	NextRunnable() Runnable[I]
}

// ActionMiddleware wraps actions to add cross-cutting concerns such as conditional execution,
// panic recovery, or logging.
type ActionMiddleware[I, O Event] interface {
	Wrap(ctx context.Context, rule Rule[I, O], action Action[I, O], in I) (Action[I, O], error)
}

// DestinationMiddleware wraps destinations to add cross-cutting concerns such as panic recovery,
// retry logic, or telemetry.
type DestinationMiddleware[I, O Event] interface {
	Wrap(ctx context.Context, rule Rule[I, O], destination Destination[O], out O) (Destination[O], error)
}

// CallbackMiddleware wraps source callbacks to add cross-cutting concerns such as conditional execution.
type CallbackMiddleware[I, O Event] interface {
	Wrap(ctx context.Context, rule *Rule[I, O], callback Callback[I], in I) (Callback[I], error)
}
