# Middleware API

API reference for middleware interfaces and built-in implementations.

## Middleware Interface

```go
type Middleware[I, O any] interface {
    WrapCallback(ctx context.Context, rule Rule, cb Callback[I]) (Callback[I], error)
    WrapAction(ctx context.Context, rule Rule, action Action[I, O]) (Action[I, O], error)
    WrapDestination(ctx context.Context, rule Rule, dest Destination[O]) (Destination[O], error)
}
```

## Built-in Middlewares

### middlewares.Panic

Panic recovery for all pipeline stages.

```go
import "github.com/emad-elsaid/firehose/middlewares"

Middlewares: []fh.Middleware[I, O]{
    &middlewares.Panic[I, O]{},
}
```

### middlewares.Slog

Structured logging via log/slog.

```go
Middlewares: []fh.Middleware[I, O]{
    &middlewares.Slog[I, O]{},
}
```

### middlewares.Parallel

Run same-source rules in parallel.

```go
import "github.com/emad-elsaid/firehose/runner"

Middlewares: []fh.Middleware[I, O]{
    &middlewares.Parallel[I, O]{
        Runner: runner.Basic{},
    },
}
```

## Creating Custom Middleware

Example logging middleware:

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
    log.Printf("[%s] Processing...", a.ruleID)
    out, err := a.next.Process(ctx, event, syms)
    log.Printf("[%s] Done", a.ruleID)
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
