# Middleware

Middlewares intercept and wrap pipeline components to add cross-cutting concerns like
logging, metrics, retry logic, or rate limiting.

## Overview

Firehose provides a unified middleware interface that can intercept three points in
the event processing pipeline:

- **Callbacks** - Event reception from sources
- **Actions** - Event transformation logic
- **Destinations** - Event output handling

## Middleware Interface

```go
type Middleware[I, O any] interface {
    WrapCallback(ctx context.Context, rule Rule, cb Callback[I]) (Callback[I], error)
    WrapAction(ctx context.Context, rule Rule, action Action[I, O]) (Action[I, O], error)
    WrapDestination(ctx context.Context, rule Rule, dest Destination[O]) (Destination[O], error)
}
```

Each method:
- Receives the original component and the `Rule` interface
- Returns a wrapped version
- Can return the original unchanged
- Executes in reverse registration order (last middleware wraps first)

## Creating Custom Middleware

### Logging Middleware Example

```go
type LoggingMiddleware[I, O any] struct{}

type loggingAction[I, O any] struct {
    ruleID string
    next   fh.Action[I, O]
}

func (a loggingAction[I, O]) Process(
    ctx context.Context,
    event I,
    syms boolexpr.Symbols,
) (O, error) {
    log.Printf("[%s] Processing event: %+v", a.ruleID, event)

    start := time.Now()
    out, err := a.next.Process(ctx, event, syms)
    duration := time.Since(start)

    if err != nil {
        log.Printf("[%s] Failed after %v: %v", a.ruleID, duration, err)
    } else {
        log.Printf("[%s] Completed in %v", a.ruleID, duration)
    }

    return out, err
}

func (m LoggingMiddleware[I, O]) WrapAction(
    ctx context.Context,
    rule fh.Rule,
    action fh.Action[I, O],
) (fh.Action[I, O], error) {
    return loggingAction[I, O]{ruleID: rule.GetID(), next: action}, nil
}

func (m LoggingMiddleware[I, O]) WrapCallback(
    ctx context.Context,
    rule fh.Rule,
    cb fh.Callback[I],
) (fh.Callback[I], error) {
    return cb, nil
}

func (m LoggingMiddleware[I, O]) WrapDestination(
    ctx context.Context,
    rule fh.Rule,
    dest fh.Destination[O],
) (fh.Destination[O], error) {
    return dest, nil
}
```

### Using Middleware

```go
rule := &fh.SQLRule[Event, Output]{
    ID:   "my_rule",
    Select: action,
    Into:   destination,
    From:   source,
    Middlewares: []fh.Middleware[Event, Output]{
        &LoggingMiddleware[Event, Output]{},
    },
}
```

## Built-in Middlewares

### Panic Recovery

Recovers from panics in callbacks, actions, and destinations:

```go
import "github.com/emad-elsaid/firehose/middlewares"

Middlewares: []fh.Middleware[I, O]{
    &middlewares.Panic[I, O]{},
}
```

**Features:**
- Catches panics in all pipeline stages
- Converts panics to error reports
- Prevents pipeline crashes
- Logs panic details

### Structured Logging

Logs events and reports using `log/slog`:

```go
Middlewares: []fh.Middleware[I, O]{
    &middlewares.Slog[I, O]{},
}
```

**Logs:**
- Event reception with source info
- Event content
- Processing results (errors)

### Parallel Execution

Runs same-source rules in parallel:

```go
import "github.com/emad-elsaid/firehose/runner"

Middlewares: []fh.Middleware[I, O]{
    &middlewares.Parallel[I, O]{
        Runner: runner.Basic{},
    },
}
```

**Features:**
- Processes events concurrently across rules
- Shared sources remain single-started
- Configurable runner implementation
- Improves throughput for CPU-bound rules

## Middleware Composition

Middlewares compose in registration order. The last middleware wraps the actual
component, so the first middleware in the slice is the outermost:

```go
Middlewares: []fh.Middleware[I, O]{
    &middlewares.Panic[I, O]{},      // Outermost (catches panics from all)
    &middlewares.Slog[I, O]{},       // Logs events and timing
    &MetricsMiddleware[I, O]{},      // Innermost (closest to actual logic)
}
```

Execution order:
1. Panic recovery (enters)
2. Slog logging (enters)
3. Metrics recording (enters)
4. **Actual action executes**
5. Metrics recording (exits)
6. Slog logging (exits)
7. Panic recovery (exits)

## Advanced Patterns

### Retry Middleware

```go
type RetryMiddleware[I, O any] struct {
    MaxAttempts int
    Delay       time.Duration
}

type retryAction[I, O any] struct {
    next        fh.Action[I, O]
    maxAttempts int
    delay       time.Duration
}

func (a retryAction[I, O]) Process(
    ctx context.Context,
    event I,
    syms boolexpr.Symbols,
) (O, error) {
    var out O
    var err error

    for attempt := 1; attempt <= a.maxAttempts; attempt++ {
        out, err = a.next.Process(ctx, event, syms)

        if err == nil {
            return out, nil
        }

        if attempt < a.maxAttempts {
            time.Sleep(a.delay)
        }
    }

    return out, err
}
```

### Metrics Middleware

```go
type MetricsMiddleware[I, O any] struct {
    Registry *prometheus.Registry
}

type metricsAction[I, O any] struct {
    next      fh.Action[I, O]
    ruleID    string
    duration  prometheus.Histogram
    errors    prometheus.Counter
}

func (a metricsAction[I, O]) Process(
    ctx context.Context,
    event I,
    syms boolexpr.Symbols,
) (O, error) {
    start := time.Now()
    out, err := a.next.Process(ctx, event, syms)

    a.duration.Observe(time.Since(start).Seconds())

    if err != nil {
        a.errors.Inc()
    }

    return out, err
}
```

### Circuit Breaker Middleware

```go
type CircuitBreakerMiddleware[I, O any] struct {
    FailureThreshold int
    ResetTimeout     time.Duration
}

type circuitBreakerAction[I, O any] struct {
    next             fh.Action[I, O]
    failures         atomic.Int32
    threshold        int32
    state            atomic.Value
    lastFailure      atomic.Value
    resetTimeout     time.Duration
}

func (a *circuitBreakerAction[I, O]) Process(
    ctx context.Context,
    event I,
    syms boolexpr.Symbols,
) (O, error) {
    state := a.state.Load().(string)

    if state == "open" {
        lastFail := a.lastFailure.Load().(time.Time)
        if time.Since(lastFail) > a.resetTimeout {
            a.state.Store("half-open")
        } else {
            var zero O
            return zero, errors.New("circuit breaker open")
        }
    }

    out, err := a.next.Process(ctx, event, syms)

    if err != nil {
        failures := a.failures.Add(1)
        a.lastFailure.Store(time.Now())

        if failures >= a.threshold {
            a.state.Store("open")
        }
    } else {
        a.failures.Store(0)
        a.state.Store("closed")
    }

    return out, err
}
```

## Best Practices

1. **Keep middlewares focused** - One concern per middleware
2. **Compose small middlewares** - Better than monolithic ones
3. **Order matters** - Panic recovery should be outermost
4. **Return unchanged when not needed** - Don't wrap unnecessarily
5. **Use context for cancellation** - Respect context deadlines
6. **Handle errors gracefully** - Don't panic in middleware code
7. **Document behavior** - Explain what each middleware does

## Next Steps

- Explore [Built-in Components](/guide/components)
- See [Examples](/examples/)
