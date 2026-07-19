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

### Pipeline Field Mapping

`SQLRule`, `ScenarioRule`, and `StreamRule` express the same pipeline: source → input
condition → action → output condition → destination. `MapReduceRule` adds an
intermediary Reduce stage for stateful accumulation.

| Stage | `SQLRule` | `ScenarioRule` (BDD) | `StreamRule` (Kafka Streams) | `MapReduceRule` |
|-------|-----------|---------------------|------------------------------|-----------------|
| Source | `From` | `When` | `Source` | `Source` |
| Input condition | `Where` | `Given` | `Filter` | `Filter` |
| Action | `Select` | `Then` | `Map` | `Map` |
| Reduce | — | — | — | `Reduce` |
| Output condition | `Having` | `GivenOutput` | `FilterOutput` | `FilterOutput` |
| Destination | `Into` | `To` | `Sink` | `Sink` |

---

## ScenarioRule

BDD-inspired rule with Given-When-Then semantics. Note the field is named
`When` (not `Give`) to align with the "when" step in BDD scenarios.

```go
type ScenarioRule[I, O any] struct {
    ID           string
    Environments []string
    When         Source[I]
    Given        Condition[I]
    Then         Action[I, O]
    GivenOutput  Condition[O]
    To           Destination[O]
    Middlewares  []Middleware[I, O]
}
```

### Fields

#### When (Source[I])
Event source that produces events of type `I`. Named after the "when" step in
BDD Given-When-Then scenarios.

#### Given (Condition[I])
Optional condition that filters input events. Equivalent to `Where` on `SQLRule`.

#### Then (Action[I, O])
Transformation that converts input events to output events. Equivalent to `Select`.

#### GivenOutput (Condition[O])
Optional condition that filters transformed output events before sending to destination.
Equivalent to `Having`.

#### To (Destination[O])
Destination that consumes output events. Equivalent to `Into`.

---

## StreamRule

Kafka Streams-inspired rule with Source/Filter/Map/FilterOutput/Sink semantics.

```go
type StreamRule[I, O any] struct {
    ID           string
    Environments []string
    Source       Source[I]
    Filter       Condition[I]
    Map          Action[I, O]
    FilterOutput Condition[O]
    Sink         Destination[O]
    Middlewares  []Middleware[I, O]
}
```

### Fields

#### Source (Source[I])
Event source that produces events of type `I`. Equivalent to `From` on `SQLRule`.

#### Filter (Condition[I])
Optional condition that filters input events. Equivalent to `Where`.

#### Map (Action[I, O])
Transformation that converts input events to output events. Equivalent to `Select`.

#### FilterOutput (Condition[O])
Optional condition that filters transformed output events before sending to destination.
Equivalent to `Having`.

#### Sink (Destination[O])
Destination that consumes output events. Equivalent to `Into`.

---

## MapReduceRule

MapReduce-inspired rule with Source/Filter/Map/Reduce/FilterOutput/Sink semantics.
Uses three type parameters: `I` (input), `M` (intermediary), `Out` (accumulated
output). The Reduce stage maintains a thread-safe accumulator via `sync.Mutex`.

```go
type MapReduceRule[I, M, Out any] struct {
    ID           string
    Environments []string
    Source       Source[I]
    Filter       Condition[I]
    Map          Action[I, M]
    Reduce       Reducer[M, Out]
    FilterOutput Condition[Out]
    Sink         Destination[Out]
    Middlewares  []Middleware[I, M]
}
```

### Fields

#### Source (Source[I])
Event source that produces events of type `I`. Equivalent to `From` on `SQLRule`.

#### Filter (Condition[I])
Optional condition that filters input events. Equivalent to `Where`.

#### Map (Action[I, M])
Transformation that converts input events of type `I` to intermediary values of type
`M`. Equivalent to `Select`.

#### Reduce (Reducer[M, Out])
Combines an intermediary value with the current accumulator to produce a new output
value. The accumulator is thread-safe and persists across events from the same source.

```go
type Reducer[M, Out any] interface {
    Reduce(ctx context.Context, value M, accumulator Out) (Out, error)
}
```

The accumulator starts at the zero value of `Out` and is updated atomically after each
successful Reduce call.

#### FilterOutput (Condition[Out])
Optional condition that filters reduced output before sending to sink. Equivalent to
`Having`.

#### Sink (Destination[Out])
Destination that consumes accumulated output events. Equivalent to `Into`.

### Accumulator Behavior

The Reduce accumulator is updated atomically before the FilterOutput check — even if
FilterOutput rejects the result, the accumulator retains the new value.

```go
// Pipeline execution order for MapReduceRule:
// 1. Source emits event I
// 2. Filter (optional) — skip if false, return ErrInputNoMatch
// 3. Map — transforms I → M
// 4. Reduce — combines M with accumulator Out → new Out
// 5. FilterOutput (optional) — skip if false, return ErrOutputNoMatch
// 6. Sink — sends Out to destination
```

---

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
    GetEnvironments() []string
    GetNext() Rule
    SetNext(n Rule)
    GetPrev() Rule
    SetPrev(p Rule)
	GetNextSameSource() Rule
	SetNextSameSource(n Rule)
	SetPrevSameSource(p Rule)
	Start(ctx context.Context) (<-chan struct{}, error)
	Init(ctx context.Context) error
}
```

## RuleError

Wraps errors with the originating rule ID.

```go
type RuleError struct {
	Rule string
	Err  error
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

Indicates an input condition (`Where` / `Given` / `Filter`) evaluated to false
(normal control flow, not an error).

```go
var ErrInputNoMatch = errors.New("no match")
```

### ErrOutputNoMatch

Indicates an output condition (`Having` / `GivenOutput` / `FilterOutput`) evaluated
to false (normal control flow, not an error).

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

### ReduceError

Wraps errors from the Reduce operation in a `MapReduceRule`.

```go
type ReduceError struct {
    Err error
}

func (e ReduceError) Error() string
func (e ReduceError) Unwrap() error
```

## Reducer Interface

Stateful accumulator for `MapReduceRule`.

```go
type Reducer[M, Out any] interface {
    Reduce(ctx context.Context, value M, accumulator Out) (Out, error)
}
```

## Next Steps

- [Source Interface](/api/sources)
- [Action Interface](/api/actions)
- [Destination Interface](/api/destinations)
- [Middleware Interface](/api/middleware)
