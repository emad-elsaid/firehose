# Conditions API

API reference for condition interfaces and built-in implementations.

## If Interface

```go
type If[I any] interface {
    Evaluate(ctx context.Context, event I, syms boolexpr.Symbols) (bool, error)
}
```

## Built-in Conditions

### ifs.Cond

Expression-based filtering using boolexpr syntax.

```go
import "github.com/emad-elsaid/firehose/ifs"

Where: ifs.Cond[OrderEvent]("amount > 1000")
```

**Supported operators:** `=`, `==`, `!=`, `<`, `<=`, `>`, `>=`, `contains`, `excludes`, `starts_with`, `ends_with`

### ifs.Func

Function adapter for custom logic.

```go
Where: ifs.Func[Event](func(ctx context.Context, event Event, syms boolexpr.Symbols) (bool, error) {
    return event.Amount > 1000, nil
})
```

### ifs.RateLimit

Throttle event processing.

```go
Where: &ifs.RateLimit[Event]{
    Limit: rate.Every(time.Second),
    Burst: 10,
}
```

### ifs.Once

Deduplicate events by ID within a time window.

```go
Where: &ifs.Once[Event]{
    Duration: 5 * time.Minute,
    Cache:    cache.NewMemory[bool](10*time.Minute, time.Minute),
}
```

### ifs.Ifs

Combine multiple conditions (AND logic).

```go
Where: ifs.Ifs[Event]{
    ifs.Cond[Event]("amount > 100"),
    &ifs.RateLimit[Event]{Limit: rate.Every(time.Second), Burst: 5},
}
```
