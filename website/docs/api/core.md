# Core Types

Core type definitions for the Firehose framework.

## SQLRule

Defines a complete event processing pipeline.

```go
type SQLRule[I, O any] struct {
    ID           string
    Environments []string
    Select       Action[I, O]
    Into         Destination[O]
    From         Source[I]
    Where        Condition[I]
    Having       Condition[O]
    Middlewares  []Middleware[I, O]
}
```

### Fields

#### ID (string)
Unique identifier for the rule. Used in logs, reports, and monitoring.

```go
ID: "order_processing"
```

#### Environments ([]string)
Optional list of environments where this rule is active. If empty, rule is active in all environments.

```go
Environments: []string{"production", "staging"}
```

Rule is included only when the `ENV` environment variable matches one of the listed values.

#### Select (Action[I, O])
Transformation that converts input events (type `I`) to output events (type `O`).

```go
Select: ProcessOrder{}
```

#### Into (Destination[O])
Destination that consumes output events of type `O`.

```go
Into: OrderDatabase{}
```

#### From (Source[I])
Event source that produces events of type `I`.

```go
From: HTTPServer{Addr: ":8080"}
```

#### Where (Condition[I])
Optional condition that filters input events. If nil, all input events pass through.

```go
Where: condition.Cond[OrderEvent]("amount > 1000")
```

#### Having (Condition[O])
Optional condition that filters transformed output events before sending to destination.

```go
Having: condition.Cond[ProcessedOrder]("status = \"ready\"")
```

#### Middlewares ([]Middleware[I, O])
Pipeline interceptors applied in reverse registration order (first middleware wraps last).

```go
Middlewares: []Middleware[I, O]{
    &middlewares.Panic[I, O]{},
    &middlewares.Slog[I, O]{},
}
```

## Source Interface

```go
type Source[T any] interface {
    Start(ctx context.Context, cb Callback[T]) (done <-chan struct{}, err error)
}
```

## Action Interface

```go
type Action[I, O any] interface {
    Process(ctx context.Context, event I, syms boolexpr.Symbols) (O, error)
}
```

## Destination Interface

```go
type Destination[T any] interface {
    Send(ctx context.Context, event T) error
}
```

## Condition Interface

```go
type Condition[I any] interface {
    Evaluate(ctx context.Context, event I, syms boolexpr.Symbols) (bool, error)
}
```

## Middleware Interface

```go
type Middleware[I, O any] interface {
    WrapCallback(ctx context.Context, rule Rule, cb Callback[I]) (Callback[I], error)
    WrapAction(ctx context.Context, rule Rule, action Action[I, O]) (Action[I, O], error)
    WrapDestination(ctx context.Context, rule Rule, dest Destination[O]) (Destination[O], error)
}
```

## Callback and ErrorHandler

```go
type Callback[I any] func(context.Context, I, ErrorHandler)

type ErrorHandler func(err error)
```

`Callback` is called by sources to deliver events to the pipeline.

`ErrorHandler` receives errors from rule execution back to the source.

## Rule

Opaque interface representing registered rules.

```go
type Rule interface {
    GetID() string
    GetSource() any
    GetNext() Rule
    SetNext(n Rule)
    GetPrev() Rule
    SetPrev(p Rule)
    GetNextSameSource() Rule
    SetNextSameSource(n Rule)
    Start(ctx context.Context) (<-chan struct{}, error)
    Init(ctx context.Context) error
    // ...
}
```

## RuleError

Wraps errors with the originating rule ID.

```go
type RuleError struct {
    Err  error
    Rule string
}

func NewRuleError(rule string, err error) error
```

## Helper Functions

```go
// EventID computes a hash-based identifier for an event.
func EventID(event any) (uint64, error)

// EventSymbols extracts symbols from an event if it implements boolexpr.Symbols.
func EventSymbols(event any) boolexpr.Symbols
```

## Error Types

### ErrInputNoMatch

Indicates a `Where` condition evaluated to false (normal control flow, not an error).

```go
var ErrInputNoMatch = errors.New("no match")
```

### ErrOutputNoMatch

Indicates a `Having` condition evaluated to false (normal control flow, not an error).

```go
var ErrOutputNoMatch = errors.New("output no match")
```

### ConditionError

Wraps errors from condition evaluation.

```go
type ConditionError struct {
    Err error
}

func (e ConditionError) Error() string
func (e ConditionError) Unwrap() error
```

### ActionError

Wraps errors from action processing.

```go
type ActionError struct {
    Err error
}

func (e ActionError) Error() string
func (e ActionError) Unwrap() error
```

### DestinationError

Wraps errors from destination operations.

```go
type DestinationError struct {
    Err error
}

func (e DestinationError) Error() string
func (e DestinationError) Unwrap() error
```

## Next Steps

- [Source Interface](/api/sources)
- [Action Interface](/api/actions)
- [Destination Interface](/api/destinations)
- [Middleware Interface](/api/middleware)
