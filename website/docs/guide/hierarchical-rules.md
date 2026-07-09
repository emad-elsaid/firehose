# Hierarchical Rules

SubRules enable hierarchical event processing where child rules inherit parent configuration while customizing their own transformations and destinations.

## Overview

Hierarchical rules solve the problem of duplicating configuration across similar rules. Child rules (SubRules) inherit:

- Event source (`From`)
- Conditions (`Where`)
- Middlewares

Child rules define their own:
- Rule ID
- Additional conditions
- Transformation (`Select`)
- Destination (`Into`)
- Their own SubRules

## Basic Example

```go
type (
    I = ProcessEvent
    O = Alert
)

parentRule := &fh.Rule[I, O]{
    ID: "production_monitoring",
    From: processMonitor,
    Where: condition.Cond[I](`env = "production"`),
    
    SubRules: []fh.Rule[I, O]{
        {
            ID:   "alert_database",
            Where:   condition.Cond[I](`name = "postgres"`),
            Select: CreateAlert{Severity: "high", Type: "database"},
            Into:   PagerDuty{},
        },
        {
            ID:   "alert_cache",
            Where:   condition.Cond[I](`name = "redis"`),
            Select: CreateAlert{Severity: "medium", Type: "cache"},
            Into:   Slack{},
        },
    },
}
```

**Effective conditions:**
- `alert_database`: `(env = "production") AND (name = "postgres")`
- `alert_cache`: `(env = "production") AND (name = "redis")`

Both SubRules share the same source (`processMonitor`) which starts only once.

## Condition Inheritance

Parent conditions combine with child conditions using logical AND:

```go
parentRule := &fh.Rule[Event, Output]{
    From: source,
    Where: condition.Cond[Event](`country = "US"`),
    
    SubRules: []fh.Rule[Event, Output]{
        {
            ID: "high_value",
            Where: condition.Cond[Event](`amount > 1000`),
            // Effective: (country = "US") AND (amount > 1000)
        },
        {
            ID: "premium_users",
            Where: condition.Cond[Event](`tier = "premium"`),
            // Effective: (country = "US") AND (tier = "premium")
        },
    },
}
```

## Source Sharing

All SubRules share the parent's source. The source starts once and events fan out to all rules:

```go
kafkaSource := &KafkaConsumer{Topic: "orders"}

parentRule := &fh.Rule[OrderEvent, any]{
    From: kafkaSource,
    
    SubRules: []fh.Rule[OrderEvent, any]{
        {ID: "email", Into: EmailService{}},
        {ID: "metrics", Into: MetricsCollector{}},
        {ID: "archive", Into: ArchiveStorage{}},
    },
}

// kafkaSource.Start() called once
// Each event delivered to all three SubRules
```

## Middleware Inheritance

Parent middlewares apply to all SubRules:

```go
parentRule := &fh.Rule[Event, Output]{
    From: source,
    Middlewares: []fh.Middleware[Event, Output]{
        &middlewares.Panic[Event, Output]{},
        &middlewares.Slog[Event, Output]{},
    },
    
    SubRules: []fh.Rule[Event, Output]{
        {
            ID: "rule1",
            // Inherits Panic and Slog middlewares
        },
        {
            ID: "rule2",
            Middlewares: []fh.Middleware[Event, Output]{
                &MetricsMiddleware[Event, Output]{},
            },
            // Has: Panic, Slog (from parent), Metrics (own)
        },
    },
}
```

Middleware order: Parent middlewares → Child middlewares

## Multi-Level Hierarchies

SubRules can have their own SubRules:

```go
root := &fh.Rule[Event, Output]{
    ID: "root",
    From: source,
    Where: condition.Cond[Event](`region = "US"`),
    
    SubRules: []fh.Rule[Event, Output]{
        {
            ID: "west_coast",
            Where: condition.Cond[Event](`state in ["CA", "OR", "WA"]`),
            
            SubRules: []fh.Rule[Event, Output]{
                {
                    ID:   "california_high_value",
                    Where:   condition.Cond[Event](`state = "CA" and amount > 5000`),
                    Select: ProcessHighValue{},
                    Into:   SpecialHandling{},
                },
                {
                    ID:   "pacific_northwest",
                    Where:   condition.Cond[Event](`state in ["OR", "WA"]`),
                    Select: ProcessNormal{},
                    Into:   StandardHandling{},
                },
            },
        },
    },
}
```

Effective condition for `california_high_value`:
```
(region = "US") AND 
(state in ["CA", "OR", "WA"]) AND 
(state = "CA" and amount > 5000)
```

## Real-World Example: API Gateway

```go
type HTTPRequest struct {
    Method string
    Path   string
    UserID string
}

apiGateway := &fh.Rule[HTTPRequest, HTTPResponse]{
    ID: "api_gateway",
    From: HTTPServer{Addr: ":8080"},
    Middlewares: []fh.Middleware[HTTPRequest, HTTPResponse]{
        &middlewares.Panic[HTTPRequest, HTTPResponse]{},
        &AuthMiddleware[HTTPRequest, HTTPResponse]{},
        &LoggingMiddleware[HTTPRequest, HTTPResponse]{},
    },
    
    SubRules: []fh.Rule[HTTPRequest, HTTPResponse]{
        {
            ID: "user_endpoints",
            Where: condition.Cond[HTTPRequest](`path starts_with "/api/users"`),
            
            SubRules: []fh.Rule[HTTPRequest, HTTPResponse]{
                {
                    ID:   "get_user",
                    Where:   condition.Cond[HTTPRequest](`method = "GET"`),
                    Select: GetUserHandler{},
                    Into:   JSONResponse{},
                },
                {
                    ID:   "create_user",
                    Where:   condition.Cond[HTTPRequest](`method = "POST"`),
                    Select: CreateUserHandler{},
                    Into:   JSONResponse{},
                },
                {
                    ID:   "update_user",
                    Where:   condition.Cond[HTTPRequest](`method = "PUT"`),
                    Select: UpdateUserHandler{},
                    Into:   JSONResponse{},
                },
            },
        },
        {
            ID: "order_endpoints",
            Where: condition.Cond[HTTPRequest](`path starts_with "/api/orders"`),
            
            SubRules: []fh.Rule[HTTPRequest, HTTPResponse]{
                {
                    ID:   "list_orders",
                    Where:   condition.Cond[HTTPRequest](`method = "GET"`),
                    Select: ListOrdersHandler{},
                    Into:   JSONResponse{},
                },
                {
                    ID:   "create_order",
                    Where:   condition.Cond[HTTPRequest](`method = "POST"`),
                    Select: CreateOrderHandler{},
                    Into:   JSONResponse{},
                },
            },
        },
    },
}
```

## Benefits

### DRY (Don't Repeat Yourself)

Before SubRules:
```go
// Duplicated source and parent condition
rule1 := &fh.Rule[Event, Output]{
    ID: "rule1",
    From: source,
    Where: condition.Cond[Event](`env = "production" and type = "A"`),
}

rule2 := &fh.Rule[Event, Output]{
    ID: "rule2",
    From: source,
    Where: condition.Cond[Event](`env = "production" and type = "B"`),
}
```

With SubRules:
```go
parent := &fh.Rule[Event, Output]{
    From: source,
    Where: condition.Cond[Event](`env = "production"`),
    SubRules: []fh.Rule[Event, Output]{
        {ID: "rule1", Where: condition.Cond[Event](`type = "A"`)},
        {ID: "rule2", Where: condition.Cond[Event](`type = "B"`)},
    },
}
```

### Centralized Configuration

Change parent configuration once, affects all children:

```go
// Add middleware to parent
parent.Middlewares = append(parent.Middlewares, &NewMiddleware{})

// All SubRules automatically get it
```

### Logical Organization

Group related rules together:

```go
monitoring := &fh.Rule[Event, Alert]{
    ID: "monitoring",
    From: systemEvents,
    SubRules: []fh.Rule[Event, Alert]{
        {ID: "cpu", ...},
        {ID: "memory", ...},
        {ID: "disk", ...},
    },
}
```

## Patterns

### Fan-Out Processing

Send same events to multiple destinations:

```go
parent := &fh.Rule[Event, Event]{
    From: source,
    SubRules: []fh.Rule[Event, Event]{
        {ID: "database", Select: actions.Identity[Event]{}, Into: Database{}},
        {ID: "cache", Select: actions.Identity[Event]{}, Into: Cache{}},
        {ID: "search", Select: actions.Identity[Event]{}, Into: SearchIndex{}},
    },
}
```

### Environment-Specific Processing

```go
parent := &fh.Rule[Event, Output]{
    From: source,
    SubRules: []fh.Rule[Event, Output]{
        {
            ID:           "prod",
            Environments: []string{"production"},
            Select:         ProductionHandler{},
            Into:           ProductionDB{},
        },
        {
            ID:           "dev",
            Environments: []string{"development"},
            Select:         DevHandler{},
            Into:           DevDB{},
        },
    },
}
```

### Progressive Filtering

```go
parent := &fh.Rule[Event, Output]{
    From: allEvents,
    Where: condition.Cond[Event](`severity >= 3`),  // Medium and above
    
    SubRules: []fh.Rule[Event, Output]{
        {
            ID: "high_severity",
            Where: condition.Cond[Event](`severity >= 4`),  // High and critical
            Into: PagerDuty{},
        },
        {
            ID: "medium_severity",
            Where: condition.Cond[Event](`severity = 3`),   // Medium only
            Into: Slack{},
        },
    },
}
```

## Best Practices

1. **Use for related rules** - Group rules that share sources and conditions
2. **Keep hierarchies shallow** - 2-3 levels maximum for clarity
3. **Document inheritance** - Comment effective conditions
4. **Test each level** - Verify parent and child behavior separately
5. **Avoid deep nesting** - Flat is better than nested
6. **Name descriptively** - IDs should indicate hierarchy

## Next Steps

- Learn about [Custom Components](/guide/custom-components)
- Explore [Testing Strategies](/guide/testing)
- See [Examples](/examples/)
