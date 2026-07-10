# API Reference

Complete API documentation for Firehose event processing framework.

## Package Import

```go
import fh "github.com/emad-elsaid/firehose"
```

## Core Functions

### Add

Registers a rule and returns an updated registry.

```go
func Add[I, O any](
    ctx context.Context,
    registry Registry,
    rule *Rule[I, O],
) (Registry, error)
```

**Parameters:**
- `ctx` - Context for rule initialization
- `registry` - Existing registry (can be `nil` for first rule)
- `rule` - Rule to register

**Returns:**
- Updated registry
- Error if rule validation fails

**Example:**

```go
registry, err := fh.Add(ctx, nil, &fh.Rule[Event, Output]{
    ID:   "my_rule",
    Select: action,
    Into:   destination,
    From:   source,
})
```

### Start

Activates all registered event sources.

```go
func Start(ctx context.Context, registry Registry, errFunc ErrorHandler)
```

**Parameters:**
- `ctx` - Context for source lifecycle
- `registry` - Registry containing rules
- `errFunc` - Handler for source startup errors

**Example:**

```go
errHandler := func(err error) {
    if err != nil && !errors.Is(err, context.Canceled) {
        log.Printf("error: %v", err)
    }
}

fh.Start(ctx, registry, errHandler)
```

### Wait

Blocks until all sources complete.

```go
func Wait(registry Registry, errFunc ErrorHandler)
```

**Parameters:**
- `registry` - Registry containing rules
- `errFunc` - Handler for source completion errors

**Example:**

```go
fh.Wait(registry, errHandler)
```

## Type Reference

See detailed documentation:

- [Core Types](/api/core)
- [Conditions](/api/conditions)
- [Actions](/api/actions)
- [Destinations](/api/destinations)
- [Sources](/api/sources)
- [Middleware](/api/middleware)
