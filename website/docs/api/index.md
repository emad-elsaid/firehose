# API Reference

Complete API documentation for Firehose event processing framework.

## Package Import

```go
import fh "github.com/emad-elsaid/firehose"
```

## Core Functions

### Add

Registers a rule and returns an updated head.

```go
func Add[I, O any](
    ctx context.Context,
    head Rule,
    rule *SQLRule[I, O],
) (Rule, error)
```

**Parameters:**
- `ctx` - Context for rule initialization
- `head` - Existing head (can be `nil` for first rule)
- `rule` - Rule to register

**Returns:**
- Updated head
- Error if rule validation fails

**Example:**

```go
head, err := fh.Add(ctx, nil, &fh.SQLRule[Event, Output]{
    ID:   "my_rule",
    Select: action,
    Into:   destination,
    From:   source,
})
```

### Start

Activates all registered event sources.

```go
func Start(ctx context.Context, head Rule, errFunc ErrorHandler)
```

**Parameters:**
- `ctx` - Context for source lifecycle
- `head` - Rule containing rules
- `errFunc` - Handler for source startup errors

**Example:**

```go
errHandler := func(err error) {
    if err != nil && !errors.Is(err, context.Canceled) {
        log.Printf("error: %v", err)
    }
}

fh.Start(ctx, head, errHandler)
for _, ch := range fh.Start(ctx, head, errHandler) {
    <-ch
}
```

## Type Reference

See detailed documentation:

- [Core Types](/api/core)
- [Conditions](/api/conditions)
- [Actions](/api/actions)
- [Destinations](/api/destinations)
- [Sources](/api/sources)
- [Middleware](/api/middleware)
