# Conditions API

API reference for condition interfaces and built-in implementations.

## If Interface

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

Throttle event processing.

```go
Where: &condition.RateLimit[Event]{
    Limit: rate.Every(time.Second),
    Burst: 10,
}
```

### condition.Once

Deduplicate events by ID within a time window.

```go
Where: &condition.Once[Event]{
    Duration: 5 * time.Minute,
    Cache:    cache.NewMemory[bool](10*time.Minute, time.Minute),
}
```

### condition.Conditions

Combine multiple conditions (AND logic).

```go
Where: condition.Conditions[Event]{
    condition.Cond[Event]("amount > 100"),
    &condition.RateLimit[Event]{Limit: rate.Every(time.Second), Burst: 5},
}
```
