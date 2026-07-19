# Core Concepts

Understanding the fundamental concepts of Firehose will help you build robust event processing pipelines.

## Events

Events are any Go type. No interface requirements. Events flow through your pipeline from source to destination.

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

To enable conditional processing, events can optionally implement the `boolexpr.Symbols` interface:

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

## Rules

Rules define complete event processing pipelines. They combine a source, optional condition, transformation, and destination.
The field order is SQL-inspired for readability:
`Select -> Into -> From -> Where -> Having` (like `SELECT ... INTO ... FROM ... WHERE ... HAVING ...`).

```go
type SQLSQLRule[I, O any] struct {
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
// ✅ Valid - types match
SQLRule[HTTPRequest, User]{
    Select: ExtractUser{},          // HTTPRequest → User
    Into:   UserDatabase{},         // consumes User
    From:   HTTPServer{},           // produces HTTPRequest
}

// ❌ Invalid - compiler error
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
    Start(ctx context.Context, cb Callback[T]) (done context.Context, err error)
}

type Callback[I any] func(context.Context, I, ReportFunc)
```

Example HTTP source:

```go
type HTTPSource struct {
    Addr string
}

func (s HTTPSource) Start(ctx context.Context, cb fh.Callback[HTTPRequest]) (context.Context, error) {
    server := &http.Server{Addr: s.Addr}

    http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
        event := HTTPRequest{Method: r.Method, Path: r.URL.Path}
        
        cb(r.Context(), event, func(report fh.Report) {
            if report.Err != nil {
                log.Printf("rule %s failed: %v", report.Rule, report.Err)
            }
        })
    })

    go server.ListenAndServe()
    return ctx, nil
}
```

### Source Fanout

When multiple rules share the same source instance, Firehose starts it only once and fans events to all rules:

```go
kafkaSource := &KafkaConsumer{Topic: "orders"}

// kafkaSource starts once, events go to both rules
reg, _ = Add(ctx, reg, &SQLRule[Event, Email]{From: kafkaSource, ...})
reg, _ = Add(ctx, reg, &SQLRule[Event, Metrics]{From: kafkaSource, ...})
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

**Supported operators:** `=`, `==`, `!=`, `<`, `<=`, `>`, `>=`, `contains`, `excludes`, `starts_with`, `ends_with`

**Logic:** `and`, `or`, `(...)`

See [boolexpr documentation](https://github.com/emad-elsaid/boolexpr) for complete syntax.

## Actions

Actions transform input events to output events:

```go
type Action[I, O any] interface {
    Process(ctx context.Context, event I, syms boolexpr.Symbols) (O, Report)
}
```

Example transformation:

```go
type ExtractUser struct{}

func (a ExtractUser) Process(
    ctx context.Context,
    req HTTPRequest,
    syms boolexpr.Symbols,
) (User, fh.Report) {
    userID := req.Headers.Get("X-User-ID")
    user := fetchUser(userID)
    return user, fh.NewReport(nil)
}
```

## Destinations

Destinations consume events and produce side effects:

```go
type Destination[T any] interface {
    Send(ctx context.Context, event T) Report
}
```

Example database writer:

```go
type DBWriter struct {
    DB *sql.DB
}

func (d DBWriter) Send(ctx context.Context, user User) fh.Report {
    _, err := d.DB.ExecContext(ctx, "INSERT INTO users ...", user.ID, user.Name)
    return fh.NewReport(err)
}
```

## Reports

Operations return `Report` values instead of panicking:

```go
type Report struct {
    Err  error  // Optional error details
    Rule string // Rule ID (set by framework)
}
```

Common report errors:

- `ErrNoMatch` - condition evaluated to false (normal control flow)
- `ConditionError` - failure while evaluating `Where` or `Having`
- `ActionError` - failure inside `Action.Process`
- `DestinationError` - failure inside `Destination.Send`

Sources receive reports through the callback's `ReportFunc` for monitoring and observability.

## Middleware

Middlewares intercept and wrap pipeline components:

```go
type Middleware[I, O any] interface {
    WrapCallback(ctx context.Context, rule *SQLSQLRule[I, O], cb Callback[I]) (Callback[I], error)
    WrapAction(ctx context.Context, rule *SQLSQLRule[I, O], action Action[I, O]) (Action[I, O], error)
    WrapDestination(ctx context.Context, rule *SQLSQLRule[I, O], dest Destination[O]) (Destination[O], error)
}
```

Middlewares apply cross-cutting concerns like logging, metrics, retry logic, or rate limiting. They compose in registration order.

## Next Steps

- Explore [Built-in Components](/guide/components)
- Learn about [Middleware](/guide/middleware)
- See [Examples](/examples/)
