package firehose

import (
	"context"

	"github.com/emad-elsaid/boolexpr"
	"github.com/mitchellh/hashstructure/v2"
)

// EventID computes a hash-based identifier for an event.
func EventID(event any) (uint64, error) {
	return hashstructure.Hash(event, hashstructure.FormatV2, nil)
}

// EventSymbols extracts symbols from an event if it implements boolexpr.Symbols.
// Events can embed boolexpr.Symbols directly to provide attribute access for condition evaluation.
// Returns a cached wrapper around the symbols for efficient repeated lookups. If the event
// doesn't implement boolexpr.Symbols, returns an empty SymbolsMap directly (no caching needed).
func EventSymbols(event any) boolexpr.Symbols {
	if symbols, ok := event.(boolexpr.Symbols); ok {
		return boolexpr.NewCachedSymbols(symbols)
	}
	return boolexpr.SymbolsMap{}
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

// If evaluates whether an event should be processed by a rule.
// It receives the context, event, and symbols extracted from the event attributes,
// and returns true if the condition is met, false otherwise, along with any error.
type If[I any] interface {
	Evaluate(ctx context.Context, event I, syms boolexpr.Symbols) (bool, error)
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

// ReportFunc receives processing reports.
type ReportFunc func(Report)

// Callback is a function type that sources use to send events to the
// engine. It takes a context, an event, and a report sink callback.
type Callback[I any] func(context.Context, I, ReportFunc)

// Runnable represents a rule that can be executed to process events.
type Runnable[I any] interface {
	Run(ctx context.Context, event I, syms boolexpr.Symbols, report ReportFunc)
	NextRunnable() Runnable[I]
}

// Middleware wraps callbacks, actions, and destinations to add cross-cutting concerns
// such as conditional execution, panic recovery, logging, retry logic, or telemetry.
type Middleware[I, O any] interface {
	WrapCallback(ctx context.Context, rule *Rule[I, O], callback Callback[I], in I) (Callback[I], error)
	WrapAction(ctx context.Context, rule *Rule[I, O], action Action[I, O], in I) (Action[I, O], error)
	WrapDestination(ctx context.Context, rule *Rule[I, O], destination Destination[O]) (Destination[O], error)
}
