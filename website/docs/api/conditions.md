# Conditions API

API reference for condition interfaces and built-in implementations.

## Condition Interface

```go
type Condition[I any] interface {
    Evaluate(ctx context.Context, event I, syms boolexpr.Symbols) (bool, error)
}
```

## Built-in Conditions

### condition.Cond

Expression-based filtering using boolexpr syntax.

```go
import "github.com/emad-elsaid/firehose/condition"

Where: condition.Cond[OrderEvent]("amount > 1000")
```

**Supported operators:** `=`, `==`, `!=`, `<`, `<=`, `>`, `>=`, `contains`, `excludes`, `starts_with`, `ends_with`

### condition.Func

Function adapter for custom logic.

```go
Where: condition.Func[Event](func(ctx context.Context, event Event, syms boolexpr.Symbols) (bool, error) {
    return event.Amount > 1000, nil
})
```

### condition.RateLimit

Throttle event processing using `golang.org/x/time/rate`.

```go
Where: &condition.RateLimit[Event]{
    Limit: rate.Every(time.Second),
    Burst: 10,
}
```

### condition.Once

Deduplicate events by `EventID` within a time window.

```go
Where: &condition.Once[Event]{
    Duration: 5 * time.Minute,
    Cache:    cache.NewMemory[string](10*time.Minute, time.Minute),
}
```

### condition.Valid

Validate event struct fields using `go-playground/validator` struct tags.

```go
Where: &condition.Valid[Event]{}
```

Returns an error if validation fails.

### condition.Conditions

Combine multiple conditions (AND logic).

```go
Where: condition.Conditions[Event]{
    condition.Cond[Event]("amount > 100"),
    &condition.RateLimit[Event]{Limit: rate.Every(time.Second), Burst: 5},
}
```
