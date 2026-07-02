Firehose
========

A type-safe event processing framework for Go. Build composable event pipelines with conditional execution,
hierarchical rules, and middleware support.


## Problem

Applications process events from various sources (HTTP requests, message queues, timers, system events, user input)
and react with side effects. Without a structured approach, event handling becomes scattered across the codebase,
difficult to test, hard to modify, and impossible to compose or reuse.

## Solution

Firehose provides a declarative framework for event processing pipelines:

**Event Source → Condition → Transformation → Destination**

- **On**: Event source producing events of a specific type
- **If**: Optional condition evaluated against event attributes
- **Then**: Transformation logic converting input events to output events
- **To**: Destination handling the output event (side effects, storage, forwarding)

Define **Rules** that combine these components with full type safety. Rules support hierarchical composition through
**SubRules** that inherit parent properties. Extend functionality with **Middlewares** that wrap any pipeline
component.


## Core Concepts

### Events

Events are any Go type. No interface requirements. Optionally implement `boolexpr.Symbols` to expose attributes
for condition evaluation:

```go
type OrderPlaced struct {
    OrderID  string
    Amount   float64
    UserTier string
}

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

### Type-Safe Pipelines

Rules enforce type safety between pipeline stages. Input event type flows from source through transformation to
destination:

```go
// HTTP request events → Order events
Rule[HTTPRequest, OrderPlaced]

// Order events → Email notifications
Rule[OrderPlaced, EmailSent]

// Timer events → Timer events (identity transformation)
Rule[TimerTick, TimerTick]
```

The compiler ensures transformations match: `Action[I, O]` must accept the source's event type and produce the
destination's event type.

### Event Source Fanout

Register multiple rules with the same source instance. The framework detects this and starts the source only once,
fanning events out to all rules that share it:

```go
kafkaSource := &KafkaConsumer{Topic: "orders"}

// Both rules share kafkaSource - it starts once, events fan out
reg, _ = AddRule(ctx, reg, &Rule[OrderEvent, Email]{On: kafkaSource, ...})
reg, _ = AddRule(ctx, reg, &Rule[OrderEvent, Metrics]{On: kafkaSource, ...})
```

Different source instances (even of the same type) start independently.

### Hierarchical Event Processing

Define rule families with `SubRules`. Child rules inherit parent's source, conditions, and middlewares while
customizing their own transformations and destinations:

```go
type (
    I = ProcessEvent
    O any
)

&Rule[I, O]{
    On: processMonitor,
    If: ifs.Cond[I](`user = "production"`),
    SubRules: []Rule[I, O]{
        {
            ID:   "alert_postgres",
            If:   ifs.Cond[I](`name = "postgres"`),
            Then: CreateAlert{Type: "database"},
            To:   PagerDuty{},
        },
        {
            ID:   "alert_nginx", 
            If:   ifs.Cond[I](`name = "nginx"`),
            Then: CreateAlert{Type: "webserver"},
            To:   PagerDuty{},
        },
    },
}
```

Both sub-rules inherit the parent condition and source. Final conditions become:
- `(user = "production") AND (name = "postgres")`
- `(user = "production") AND (name = "nginx")`

### Event Processing Middleware

Middlewares intercept and wrap three points in the pipeline: callbacks (event reception), actions (transformation),
and destinations (output). Apply cross-cutting concerns like logging, metrics, retry logic, or rate limiting:

```go
type LoggingMiddleware[I, O any] struct{}

type loggingAction[I, O any] struct {
    ruleID string
    next   Action[I, O]
}

func (a loggingAction[I, O]) Process(ctx context.Context, event I, syms boolexpr.Symbols) (O, Report) {
    log.Printf("Processing event in rule %s", a.ruleID)
    out, report := a.next.Process(ctx, event, syms)
    log.Printf("Rule %s completed (err=%v)", a.ruleID, report.Err)

    return out, report
}

func (m LoggingMiddleware[I, O]) WrapAction(
    _ context.Context,
    rule *Rule[I, O],
    action Action[I, O],
) (Action[I, O], error) {
    return loggingAction[I, O]{ruleID: rule.ID, next: action}, nil
}
```

Middlewares compose in registration order (first wraps later middlewares).

### Event Processing Reports

Operations return `Report` values instead of panicking. Reports communicate errors and control flow:

```go
type Report struct {
    Err  error  // Optional error details and control-flow signals
    Rule string // Rule ID (set by framework)
}
```

Sources receive reports through the callback's `ReportFunc` argument, enabling monitoring and observability.


## Features

- ✅ **Type-safe event pipelines** - Generic types ensure compile-time correctness
- ✅ **Any Go type as event** - No interface requirements, works with existing types
- ✅ **Declarative conditions** - Boolean expressions via `boolexpr` library
- ✅ **Hierarchical composition** - SubRules inherit and extend parent rules
- ✅ **Unified middleware** - Single interface for callbacks, transformations, destinations
- ✅ **Source fanout optimization** - Shared sources start once, distribute to all rules
- ✅ **Context propagation** - Full context.Context support for cancellation and values
- ✅ **Report-based flow control** - Structured error handling via typed errors
- ✅ **Struct validation** - Declarative validation with `go-playground/validator`


## Built-in Components

Firehose ships with reusable building blocks under subpackages.

### Conditions (`ifs`)

- `ifs.Func[I](func(ctx, event, syms) (bool, error) { ... })` — function adapter for conditions
- `ifs.Cond[I]("...")` — expression-based filtering using `boolexpr`
- `&ifs.RateLimit[I]{Limit: ..., Burst: ...}` — throttle event processing
- `&ifs.Once[I]{Duration: ..., Cache: ...}` — deduplicate events by `EventID`
- `ifs.Ifs[I]{...}` — evaluate multiple conditions in sequence

### Actions (`actions`)

- `actions.Func[I, O](func(ctx, event, syms) (O, fh.Report) { ... })` — function adapter for actions
- `&actions.Cache[I, O]{Action: ..., Cache: ..., TTL: ...}` — memoize action output per event
- `actions.Chain[I, M, O]{...}` — compose two actions (`I -> M -> O`)
- `actions.Chain3[I, A, B, O]{...}` — compose three actions
- `actions.Chain4[I, A, B, C, O]{...}` — compose four actions
- `actions.Chain5[I, A, B, C, D, O]{...}` — compose five actions
- `&actions.RoundRobin[I, O]{Actions: ...}` — dispatch events across actions in sequence
- `&actions.Random[I, O]{Actions: ...}` — dispatch events to a random action

### Destinations (`destinations`)

- `destinations.Func[T](func(ctx, event) fh.Report { ... })` — function adapter for destinations
- `destinations.Fanout[T]{Destinations: ...}` — send to all destinations
- `&destinations.RoundRobin[T]{Destinations: ...}` — send in round-robin order
- `&destinations.Random[T]{Destinations: ...}` — send to a random destination

### Sources (`sources`)

- `sources.Func[T](func(ctx, cb) (context.Context, error) { ... })` — function adapter for sources

### Cache Storage (`cache`)

- `cache.NewMemory[O](defaultTTL, cleanupInterval)` — in-memory cache backend

### Middlewares (`middlewares`)

- `&middlewares.Panic[I, O]{}` — panic recovery for callback/action/destination
- `&middlewares.Slog[I, O]{}` — structured event/report logging via `log/slog`
- `&middlewares.Parallel[I, O]{Runner: runner.Basic{}}` — run same-source rules in parallel

## Environment-Specific Rules

Rules can be enabled only in selected environments:

```go
rule := &fh.Rule[Event, Out]{
    ID:           "billing/charge",
    Environments: []string{"production", "staging"},
    On:           source,
    Then:         action,
    To:           destination,
}
```

`AddRule` includes such a rule only when `ENV` matches one of `Environments`.
If `Environments` is empty, the rule is always active.

## Reports and Error Classification

Each rule execution emits a `Report`:

```go
type Report struct {
    Rule string
    Err  error
}
```

Common report errors:

- `ErrNoMatch` — condition evaluated to false (normal control flow)
- `ConditionError` — failure while evaluating `If`
- `ActionError` — failure inside `Action.Process`
- `DestinationError` — failure inside `Destination.Send`

Use `errors.Is` / `errors.As` to classify reports in monitoring code.

## Building Event Sources, Transformations, and Destinations

The framework defines three core interfaces you implement for custom event processing.

### Event Sources

Sources produce events and send them to a callback function:

```go
type Source[T any] interface {
    Start(ctx context.Context, cb Callback[T]) (done context.Context, err error)
}
```

Example - HTTP event source:

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

### Event Transformations

Actions transform input events to output events:

```go
type Action[I, O any] interface {
    Process(ctx context.Context, event I, syms boolexpr.Symbols) (O, Report)
}
```

Example - Extract user data:

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

### Event Destinations

Destinations consume events and produce side effects:

```go
type Destination[T any] interface {
    Send(ctx context.Context, event T) Report
}
```

Example - Database writer:

```go
type DBWriter struct {
    DB *sql.DB
}

func (d DBWriter) Send(ctx context.Context, user User) fh.Report {
    _, err := d.DB.ExecContext(ctx, "INSERT INTO users ...", user.ID, user.Name)
    if err != nil {
        return fh.NewReport(err)
    }
    return fh.NewReport(nil)
}
```


## Quick Start

Process timer events during business hours:

```go
package main

import (
    "context"
    "errors"
    "fmt"
    "log"
    "os"
    "os/signal"
    "time"

    "github.com/emad-elsaid/boolexpr"
    fh "github.com/emad-elsaid/firehose"
    "github.com/emad-elsaid/firehose/ifs"
)

// 1. Define your event type
type Tick struct {
    Time time.Time
}

// 2. Make it conditionally evaluable (optional)
func (t Tick) Get(key string) (any, error) {
    if key == "hour" {
        return t.Time.Hour(), nil
    }
    return nil, fmt.Errorf("unknown symbol: %s", key)
}

// 3. Implement an event source
type Timer struct {
    Interval time.Duration
}

func (t Timer) Start(ctx context.Context, cb fh.Callback[Tick]) (context.Context, error) {
    go func() {
        ticker := time.NewTicker(t.Interval)
        defer ticker.Stop()
        for {
            select {
            case <-ctx.Done():
                return
            case now := <-ticker.C:
                cb(ctx, Tick{Time: now}, func(report fh.Report) {
                    if report.Err != nil {
                        log.Printf("rule %s failed: %v", report.Rule, report.Err)
                    }
                })
            }
        }
    }()
    return ctx, nil
}

// 4. Implement a transformation
type FormatTime struct{}

func (FormatTime) Process(ctx context.Context, t Tick, _ boolexpr.Symbols) (string, fh.Report) {
    return t.Time.Format("15:04:05"), fh.NewReport(nil)
}

// 5. Implement a destination
type Printer struct{}

func (Printer) Send(ctx context.Context, msg string) fh.Report {
    println(msg)
    return fh.NewReport(nil)
}

func main() {
    ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
    defer stop()

    // 6. Define a rule with a condition
    rule := &fh.Rule[Tick, string]{
        ID:   "print_business_hours",
        On:   Timer{Interval: 1 * time.Second},
        If:   ifs.Cond[Tick]("hour >= 9 and hour < 17"),
        Then: FormatTime{},
        To:   Printer{},
    }

    // 7. Register and start
    registry, _ := fh.AddRule(ctx, nil, rule)

    errHandler := func(err error) {
        if err != nil && !errors.Is(err, context.Canceled) {
            log.Printf("engine error: %v", err)
        }
    }

    fh.Start(ctx, registry, errHandler)
    fh.Wait(registry, errHandler)
}
```


## Conditional Event Processing

Use `ifs.Cond` to filter events based on their attributes. Events must implement `boolexpr.Symbols` interface:

```go
type OrderEvent struct {
    Amount   float64
    Country  string
    Premium  bool
}

func (o OrderEvent) Get(key string) (any, error) {
    switch key {
    case "amount":
        return o.Amount, nil
    case "country":
        return o.Country, nil
    case "premium":
        return o.Premium, nil
    default:
        return nil, fmt.Errorf("unknown symbol: %s", key)
    }
}

// Only process high-value orders
If: ifs.Cond[OrderEvent]("amount > 1000")

// Geographic filtering
If: ifs.Cond[OrderEvent](`country = "US" or country = "CA"`)

// Complex conditions
If: ifs.Cond[OrderEvent]("premium = true and amount > 500")
```

**Operators:** `=`, `==`, `!=`, `<`, `<=`, `>`, `>=`, `contains`, `excludes`, `starts_with`, `ends_with`

**Logic:** `and`, `or`, `(...)`

**Types:** Numbers, strings, booleans, slices

See [boolexpr documentation](https://github.com/emad-elsaid/boolexpr) for complete syntax.


## API Reference

### Core Types

```go
// Rule defines a complete event processing pipeline
type Rule[I, O any] struct {
    ID           string             // Unique identifier
    Environments []string           // Active only when ENV matches; empty = all environments
    On           Source[I]          // Event source
    If           If[I]              // Optional filter condition
    Then         Action[I, O]       // Event transformation
    To           Destination[O]     // Output handler
    SubRules     []Rule[I, O]       // Child rules (inherit parent properties)
    Middlewares  []Middleware[I, O] // Pipeline interceptors
}

// Source produces events
type Source[T any] interface {
    Start(ctx context.Context, cb Callback[T]) (done context.Context, err error)
}

// Callback receives source events and a report sink function
type Callback[I any] func(context.Context, I, ReportFunc)

// ReportFunc receives processing reports
type ReportFunc func(Report)

// Action transforms events
type Action[I, O any] interface {
    Process(ctx context.Context, event I, syms boolexpr.Symbols) (O, Report)
}

// Destination consumes events
type Destination[T any] interface {
    Send(ctx context.Context, event T) Report
}

// If filters events based on conditions
type If[I any] interface {
    Evaluate(ctx context.Context, event I, syms boolexpr.Symbols) (bool, error)
}

// Middleware intercepts pipeline components
type Middleware[I, O any] interface {
    WrapCallback(ctx context.Context, rule *Rule[I, O], callback Callback[I]) (Callback[I], error)
    WrapAction(ctx context.Context, rule *Rule[I, O], action Action[I, O]) (Action[I, O], error)
    WrapDestination(ctx context.Context, rule *Rule[I, O], destination Destination[O]) (Destination[O], error)
}

// Report communicates operation results
type Report struct {
    Err  error
    Rule string // Set by framework
}
```

### Core Functions

```go
// AddRule registers a rule and returns updated registry
func AddRule[I, O any](
    ctx context.Context,
    registry Registry,
    rule *Rule[I, O],
) (Registry, error)

// ErrorHandler receives errors from Start/Wait
type ErrorHandler func(error)

// Start activates all registered event sources
func Start(ctx context.Context, registry Registry, errFunc ErrorHandler)

// Wait blocks until all sources complete
func Wait(registry Registry, errFunc ErrorHandler)
```

### Event Symbol Interface

Events optionally implement this interface for conditional processing:

```go
type Symbols interface {
    Get(key string) (any, error)
}
```

For convenience, you can embed `boolexpr.SymbolsMap` which implements this interface:

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


## Example: Hierarchical Event Processing

Process system events with inherited filtering:

```go
type (
    I = ProcessEvent
    O = Alert
)

processMonitor := &ProcessMonitor{PollInterval: 1 * time.Second}

parentRule := &fh.Rule[I, O]{
    ID:   "production_alerts",
    On:   processMonitor,
    If:   ifs.Cond[I](`env = "production" and user = "app"`),
    SubRules: []fh.Rule[I, O]{
        {
            ID:   "database_alert",
            If:   ifs.Cond[I](`name = "postgres"`),
            Then: CreateAlert{Severity: "high", Type: "database"},
            To:   PagerDuty{},
        },
        {
            ID:   "cache_alert",
            If:   ifs.Cond[I](`name = "redis"`),
            Then: CreateAlert{Severity: "medium", Type: "cache"},
            To:   PagerDuty{},
        },
        {
            ID:   "web_alert",
            If:   ifs.Cond[I](`name = "nginx"`),
            Then: CreateAlert{Severity: "critical", Type: "webserver"},
            To:   PagerDuty{},
        },
    },
}

// All SubRules inherit: processMonitor source and production environment filter
// Final effective conditions:
//   database_alert: (env="production" AND user="app") AND (name="postgres")
//   cache_alert:    (env="production" AND user="app") AND (name="redis")
//   web_alert:      (env="production" AND user="app") AND (name="nginx")

registry, _ := fh.AddRule(ctx, nil, parentRule)
```


## Design Principles

- **Event-first architecture** - Everything revolves around event types and their flow
- **Minimal core concepts** - Five interfaces: Source, If, Action, Destination, Middleware
- **Complete type safety** - Generics ensure correctness from source to destination
- **Separation of concerns** - Components define logic, rules define composition
- **Declarative over imperative** - Describe event flows, not execution details
- **Reusability by default** - Share sources, transformations, and destinations across rules
- **Hierarchical composition** - SubRules enable DRY event processing patterns
- **Production-ready validation** - Struct validation and extensive linting (50+ rules)


## Use Cases

**Event-Driven Microservices**
- HTTP request routing and handling
- gRPC stream processing
- WebSocket event distribution

**Stream Processing**
- Message queue consumers (Kafka, RabbitMQ, NATS)
- Real-time chat processing
- Log aggregation and filtering

**System Monitoring**
- Process lifecycle tracking
- File system watching
- Performance metric collection

**Business Process Automation**
- Workflow orchestration
- Rule-based decision engines
- Event-driven ETL pipelines

**Interactive Systems**
- Game input handling
- UI event processing
- Hardware device integration


## License

See [LICENSE](LICENSE) file.
