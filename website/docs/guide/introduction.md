# Introduction

Firehose is a type-safe event processing framework for Go that enables you to build composable event pipelines with conditional execution, hierarchical rules, and middleware support.

## The Problem

Applications process events from various sources (HTTP requests, message queues, timers, system events, user input) and react with side effects. Without a structured approach, event handling becomes:

- **Scattered** across the codebase
- **Difficult to test** in isolation
- **Hard to modify** without breaking things
- **Impossible to compose** or reuse

## The Solution

Firehose provides a declarative framework for event processing pipelines:

```
Event Source → Condition → Transformation → Destination
```

Each stage is:

- **On**: Event source producing events of a specific type
- **If**: Optional condition evaluated against event attributes
- **Then**: Transformation logic converting input events to output events
- **To**: Destination handling the output event (side effects, storage, forwarding)

## Key Features

### Type Safety

Rules enforce type safety between pipeline stages. The compiler ensures transformations match:

```go
// HTTP request events → Order events
Rule[HTTPRequest, OrderPlaced]

// Order events → Email notifications
Rule[OrderPlaced, EmailSent]
```

### Event Source Fanout

Register multiple rules with the same source instance. The framework detects this and starts the source only once, fanning events out to all rules:

```go
kafkaSource := &KafkaConsumer{Topic: "orders"}

// Both rules share kafkaSource - it starts once, events fan out
reg, _ = AddRule(ctx, reg, &Rule[OrderEvent, Email]{On: kafkaSource, ...})
reg, _ = AddRule(ctx, reg, &Rule[OrderEvent, Metrics]{On: kafkaSource, ...})
```

### Hierarchical Composition

Define rule families with `SubRules`. Child rules inherit parent's source, conditions, and middlewares:

```go
&Rule[ProcessEvent, any]{
    On: processMonitor,
    If: ifs.Cond[ProcessEvent](`env = "production"`),
    SubRules: []Rule[ProcessEvent, any]{
        {
            ID:   "alert_postgres",
            If:   ifs.Cond[ProcessEvent](`name = "postgres"`),
            Then: CreateAlert{Type: "database"},
            To:   PagerDuty{},
        },
        {
            ID:   "alert_nginx", 
            If:   ifs.Cond[ProcessEvent](`name = "nginx"`),
            Then: CreateAlert{Type: "webserver"},
            To:   PagerDuty{},
        },
    },
}
```

Both sub-rules inherit the parent condition. Final conditions become:
- `(env = "production") AND (name = "postgres")`
- `(env = "production") AND (name = "nginx")`

### Middleware System

Apply cross-cutting concerns like logging, metrics, retry logic, or rate limiting:

```go
type LoggingMiddleware[I, O any] struct{}

func (m LoggingMiddleware[I, O]) WrapAction(
    ctx context.Context,
    rule *Rule[I, O],
    action Action[I, O],
) (Action[I, O], error) {
    return loggingAction[I, O]{ruleID: rule.ID, next: action}, nil
}
```

## Next Steps

- [Quick Start](/guide/quick-start) - Build your first event pipeline
- [Core Concepts](/guide/concepts) - Deep dive into events, rules, and components
- [API Reference](/api/) - Complete API documentation
