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
condition, transformation, and destination.
The field order is SQL-inspired for readability:
`Select -> Into -> From -> Where -> Having` (like
`SELECT ... INTO ... FROM ... WHERE ... HAVING ...`).

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
```

### Type Safety

Rules are generic over input (`I`) and output (`O`) types. The compiler ensures:

- `Source[I]` produces events of type `I`
- `Condition[I]` evaluates conditions on type `I` (used by `Where`)
- `Action[I, O]` transforms `I` to `O` (used by `Select`)
- `Destination[O]` consumes events of type `O` (used by `Into`)

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

- `ErrInputNoMatch` — `Where` condition evaluated to false (normal control flow)
- `ErrOutputNoMatch` — `Having` condition evaluated to false (normal control flow)
- `ConditionError` — failure while evaluating `Where` or `Having`
- `ActionError` — failure inside `Action.Process`
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

1. Evaluate `Where` condition — skip and return `ErrInputNoMatch` if false
2. Execute `Select` action — transform input to output
3. Evaluate `Having` condition — skip and return `ErrOutputNoMatch` if false
4. Send to `Into` destination

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
