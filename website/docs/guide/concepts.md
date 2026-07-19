# Core Concepts

Understanding the fundamental concepts of Firehose will help you build robust event
processing pipelines.

## Events

Events are any Go type. No interface requirements. Events flow through your pipeline
from source to destination.

```go
// Simple event
type Click struct {
    X, Y int
}

// Complex event
type OrderPlaced struct {
    OrderID   string
    Amount    float64
    UserTier  string
    Timestamp time.Time
}
```

### Event Symbols

To enable conditional processing, events can optionally implement the
`boolexpr.Symbols` interface:

```go
func (o OrderPlaced) Get(key string) (any, error) {
    switch key {
    case "amount":
        return o.Amount, nil
    case "tier":
        return o.UserTier, nil
    default:
        return nil, fmt.Errorf("unknown symbol: %s", key)
    }
}
```

Alternatively, use `boolexpr.SymbolsMap` for convenience:

```go
type MyEvent struct {
    boolexpr.SymbolsMap
}

event := MyEvent{
    SymbolsMap: boolexpr.SymbolsMap{
        "count": 42,
        "name":  "example",
    },
}
```

### Event Helpers

Firehose provides helper functions for working with events:

```go
// EventID computes a hash-based identifier for an event.
id, err := fh.EventID(event)

// EventSymbols extracts symbols from an event if it implements boolexpr.Symbols.
// Returns a cached wrapper for efficient repeated lookups.
syms := fh.EventSymbols(event)
```

## Rules

Rules define complete event processing pipelines. They combine a source, optional
condition, transformation, and destination. Firehose provides four rule types
with the same pipeline but different naming conventions:

| Stage | `SQLRule` | `ScenarioRule` | `StreamRule` | `MapReduceRule` |
|-------|-----------|----------------|--------------|-----------------|
| Source | `From` | `Give` | `Source` | `Source` |
| Input filter | `Where` | `Given` | `Filter` | `Filter` |
| Transform | `Select` | `Then` | `Map` | `Map` |
| Reduce | — | — | — | `Reduce` |
| Output filter | `Having` | `GivenOutput` | `FilterOutput` | `FilterOutput` |
| Sink | `Into` | `To` | `Sink` | `Sink` |

```go
type SQLRule[I, O any] struct {
    ID           string             // Unique identifier
    Environments []string           // Active only when ENV matches
    Select       Action[I, O]       // Event transformation
    Into         Destination[O]     // Output handler
    From         Source[I]          // Event source
    Where        Condition[I]       // Optional filter condition
    Having       Condition[O]       // Optional post-transform condition
    Middlewares  []Middleware[I, O] // Pipeline interceptors
}

type ScenarioRule[I, O any] struct {
    ID           string
    Environments []string
    Give         Source[I]          // Event source
    Given        Condition[I]       // Input condition
    Then         Action[I, O]       // Transformation
    GivenOutput  Condition[O]       // Output condition
    To           Destination[O]     // Output handler
    Middlewares  []Middleware[I, O]
}

type StreamRule[I, O any] struct {
    ID           string
    Environments []string
    Source       Source[I]          // Event source
    Filter       Condition[I]       // Input condition
    Map          Action[I, O]       // Transformation
    FilterOutput Condition[O]       // Output condition
    Sink         Destination[O]     // Output handler
    Middlewares  []Middleware[I, O]
}

type MapReduceRule[I, M, Out any] struct {
    ID           string
    Environments []string
    Source       Source[I]          // Event source
    Filter       Condition[I]       // Input condition
    Map          Action[I, M]       // Transform I → M
    Reduce       Reducer[M, Out]    // Accumulate M into Out
    FilterOutput Condition[Out]     // Output condition
    Sink         Destination[Out]   // Output handler
    Middlewares  []Middleware[I, M]
}
```

`MapReduceRule` differs from the other three rule types: it uses **three** type
parameters (`I`, `M`, `Out`) and introduces the `Reduce` stage for stateful
accumulation. The `Reducer[M, Out]` interface combines each intermediary value
with the current accumulator:

```go
type Reducer[M, Out any] interface {
    Reduce(ctx context.Context, value M, accumulator Out) (Out, error)
}
```

The accumulator starts at the zero value of `Out` and is updated atomically,
thread-safe across concurrent events from the same source.

### Type Safety

`SQLRule`, `ScenarioRule`, and `StreamRule` are generic over input (`I`) and output
(`O`) types. `MapReduceRule` adds an intermediary type `M` and an accumulator type
`Out`. The compiler ensures:

- `Source[I]` produces events of type `I`
- `Condition[I]` evaluates conditions on type `I`
- `Action[I, O]` transforms `I` to `O`
- `Destination[O]` consumes events of type `O`
- `Reducer[M, Out]` combines `M` with the current `Out` accumulator

```go
// Valid - types match
SQLRule[HTTPRequest, User]{
    Select: ExtractUser{},          // HTTPRequest → User
    Into:   UserDatabase{},         // consumes User
    From:   HTTPServer{},           // produces HTTPRequest
}

// Invalid - compiler error
SQLRule[HTTPRequest, User]{
    Select: ExtractUser{},          // HTTPRequest → User
    Into:   EmailService{},         // expects Email, not User
    From:   HTTPServer{},           // produces HTTPRequest
}

// MapReduceRule with three type parameters
MapReduceRule[Order, Metrics, Report]{
    Map:    OrderToMetrics{},       // Order → Metrics
    Reduce: MetricsAccumulator{},   // Metrics + Report → Report
    Sink:   ReportPublisher{},      // consumes Report
}
```

### Choosing a Rule Type

Which rule type to use is a matter of convention — `SQLRule`, `ScenarioRule`, and
`StreamRule` are functionally identical and can be mixed in the same pipeline.
`MapReduceRule` adds stateful accumulation for use cases like rolling metrics,
event counting, or session aggregation:

```go
source := &KafkaConsumer{Topic: "orders"}

// SQL convention
head, _ = Add(ctx, head, &SQLRule[Event, Email]{From: source, ...})
// BDD convention
head, _ = Add(ctx, head, &ScenarioRule[Event, Metrics]{Give: source, ...})
// Kafka Streams convention
head, _ = Add(ctx, head, &StreamRule[Event, Audit]{Source: source, ...})
// MapReduce convention (3 type params)
head, _ = Add(ctx, head, &MapReduceRule[Event, Metric, Summary]{Source: source, ...})
```

## Sources

Sources produce events and send them to a callback function:

```go
type Source[T any] interface {
    Start(ctx context.Context, cb Callback[T]) (done <-chan struct{}, err error)
}

type Callback[I any] func(context.Context, I, ErrorHandler)

type ErrorHandler func(err error)
```

Example HTTP source:

```go
type HTTPSource struct {
    Addr string
}

func (s HTTPSource) Start(ctx context.Context, cb fh.Callback[HTTPRequest]) (<-chan struct{}, error) {
    server := &http.Server{Addr: s.Addr}

    http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
        event := HTTPRequest{Method: r.Method, Path: r.URL.Path}

        cb(r.Context(), event, func(err error) {
            if err != nil {
                log.Printf("rule failed: %v", err)
            }
        })
    })

    go server.ListenAndServe()
    return ctx.Done(), nil
}
```

### Source Fanout

When multiple rules share the same source instance, Firehose starts it only once and
fans events to all rules:

```go
kafkaSource := &KafkaConsumer{Topic: "orders"}

// kafkaSource starts once, events go to both rules
head, _ = Add(ctx, head, &SQLRule[Event, Email]{From: kafkaSource, ...})
head, _ = Add(ctx, head, &SQLRule[Event, Metrics]{From: kafkaSource, ...})
```

Different source instances start independently.

## Conditions

Conditions filter events based on their attributes:

```go
type Condition[I any] interface {
    Evaluate(ctx context.Context, event I, syms boolexpr.Symbols) (bool, error)
}
```

Use `condition.Cond` for expression-based filtering:

```go
// Simple condition
Where: condition.Cond[OrderEvent]("amount > 1000")

// Complex condition
Where: condition.Cond[OrderEvent]("premium = true and amount > 500")

// Geographic filtering
Where: condition.Cond[OrderEvent](`country = "US" or country = "CA"`)
```

**Supported operators:** `=`, `==`, `!=`, `<`, `<=`, `>`, `>=`, `contains`,
`excludes`, `starts_with`, `ends_with`

**Logic:** `and`, `or`, `(...)`

See [boolexpr documentation](https://github.com/emad-elsaid/boolexpr) for complete
syntax.

## Actions

Actions transform input events to output events:

```go
type Action[I, O any] interface {
    Process(ctx context.Context, event I, syms boolexpr.Symbols) (O, error)
}
```

Example transformation:

```go
type ExtractUser struct{}

func (a ExtractUser) Process(
    ctx context.Context,
    req HTTPRequest,
    syms boolexpr.Symbols,
) (User, error) {
    userID := req.Headers.Get("X-User-ID")
    user := fetchUser(userID)
    return user, nil
}
```

## Destinations

Destinations consume events and produce side effects:

```go
type Destination[T any] interface {
    Send(ctx context.Context, event T) error
}
```

Example database writer:

```go
type DBWriter struct {
    DB *sql.DB
}

func (d DBWriter) Send(ctx context.Context, user User) error {
    _, err := d.DB.ExecContext(ctx, "INSERT INTO users ...", user.ID, user.Name)
    return err
}
```

## Error Handling

Firehose uses typed errors for classifying failures. Errors are wrapped with
`RuleError` to identify the originating rule:

```go
type RuleError struct {
    Rule string
    Err  error
}

func NewRuleError(rule string, err error) error
```

Common sentinel and wrapper errors:

- `ErrInputNoMatch` — input condition (`Where` / `Given` / `Filter`) evaluated
   to false (normal control flow)
- `ErrOutputNoMatch` — output condition (`Having` / `GivenOutput` / `FilterOutput`)
   evaluated to false (normal control flow)
- `ConditionError` — failure while evaluating a condition
- `ActionError` — failure inside `Action.Process`
- `ReduceError` — failure inside `Reducer.Reduce` (MapReduceRule only)
- `DestinationError` — failure inside `Destination.Send`

Sources receive errors through the callback's `ErrorHandler` for monitoring and
observability.

```go
cb(ctx, event, func(err error) {
    var ruleErr fh.RuleError
    if errors.As(err, &ruleErr) {
        log.Printf("rule %s failed: %v", ruleErr.Rule, ruleErr.Err)
    }
})
```

## Rule Execution

When a source invokes the callback, the rule executes these steps in order:

1. Evaluate input condition (`Where` / `Given` / `Filter`) — skip and return
   `ErrInputNoMatch` if false
2. Execute action (`Select` / `Then` / `Map`) — transform input to output
3. **Reduce** (MapReduceRule only) — combine intermediary value with thread-safe
   accumulator
4. Evaluate output condition (`Having` / `GivenOutput` / `FilterOutput`) — skip
   and return `ErrOutputNoMatch` if false
5. Send to destination (`Into` / `To` / `Sink`)

Rules with the same source form a linked list. Each rule in the chain executes
independently.

## Middleware

Middlewares intercept and wrap pipeline components:

```go
type Middleware[I, O any] interface {
    WrapCallback(ctx context.Context, rule Rule, cb Callback[I]) (Callback[I], error)
    WrapAction(ctx context.Context, rule Rule, action Action[I, O]) (Action[I, O], error)
    WrapDestination(ctx context.Context, rule Rule, dest Destination[O]) (Destination[O], error)
}
```

Middlewares apply cross-cutting concerns like logging, metrics, retry logic, or rate
limiting. They compose in reverse registration order (first middleware wraps last).

## EventID Helper

Firehose computes a hash-based identifier for any event:

```go
func EventID(event any) (uint64, error)
```

This is used internally by `condition.Once` and `actions.Cache` to uniquely identify
events.

## Next Steps

- Explore [Built-in Components](/guide/components)
- Learn about [Middleware](/guide/middleware)
- See [Examples](/examples/)
