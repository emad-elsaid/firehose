# Best Practices

Recommended patterns and practices for building robust event processing pipelines with Firehose.

## Rule Design

### Keep Rules Focused

Each rule should have a single, clear responsibility:

```go
// Good - focused responsibility
rule := &fh.SQLRule[OrderEvent, Email]{
    ID:   "order_confirmation_email",
    Select: CreateConfirmationEmail{},
    Into:   emailService,
    Where:   condition.Cond[OrderEvent](`status = "completed"`),
    From:   orderSource,
}

// Bad - multiple responsibilities
rule := &fh.SQLRule[OrderEvent, any]{
    ID: "order_processor",
    // Tries to do too much in one rule
}
```

### Use Descriptive IDs

Rule IDs should clearly indicate purpose:

```go
// Good
ID: "high_value_order_pagerduty_alert"

// Bad
ID: "rule1"
```

### Validate Early

Use conditions to filter invalid events early:

```go
Where: condition.Conditions[Event]{
    condition.Cond[Event](`amount > 0`),
    condition.Cond[Event](`user_id != ""`),
    ValidateSchema{},
}
```

## Component Design

### Keep Actions Pure

Actions should be deterministic and stateless:

```go
// Good - pure function
type CalculateTotal struct{}

func (c CalculateTotal) Process(ctx context.Context, order Order, _ boolexpr.Symbols) (float64, error) {
    total := order.Subtotal + order.Tax - order.Discount
    return total, nil
}

// Bad - has hidden state
type CalculateTotal struct {
    cache map[string]float64 // State!
}
```

### Handle Errors Gracefully

Return errors, don't panic:

```go
// Good
func (a MyAction) Process(ctx context.Context, event Event, _ boolexpr.Symbols) (Output, error) {
    result, err := processEvent(event)
    if err != nil {
        return Output{}, fmt.Errorf("processing failed: %w", err)
    }
    return result, nil
}

// Bad
func (a MyAction) Process(ctx context.Context, event Event, _ boolexpr.Symbols) (Output, error) {
    result := processEvent(event) // Panics on error!
    return result, nil
}
```

### Respect Context

Always check context cancellation:

```go
func (s MySource) Start(ctx context.Context, cb fh.Callback[Event]) (<-chan struct{}, error) {
    go func() {
        ticker := time.NewTicker(time.Second)
        defer ticker.Stop()

        for {
            select {
            case <-ctx.Done():
                return // Respects cancellation
            case <-ticker.C:
                cb(ctx, Event{}, nil)
            }
        }
    }()
    return ctx.Done(), nil
}
```

## Performance

### Use Source Fanout

Share sources across rules instead of creating duplicates:

```go
// Good - source shared, starts once
kafkaSource := &KafkaConsumer{Topic: "orders"}

head, _ = fh.Add(ctx, head, &fh.SQLRule[Event, Email]{From: kafkaSource, ...})
head, _ = fh.Add(ctx, head, &fh.SQLRule[Event, Metrics]{From: kafkaSource, ...})

// Bad - creates separate sources
head, _ = fh.Add(ctx, head, &fh.SQLRule[Event, Email]{
    From: &KafkaConsumer{Topic: "orders"},
})
head, _ = fh.Add(ctx, head, &fh.SQLRule[Event, Metrics]{
    From: &KafkaConsumer{Topic: "orders"},
})
```

### Cache Expensive Operations

Use `actions.Cache` for expensive computations:

```go
Select: &actions.Cache[Event, Result]{
    Action: ExpensiveAPICall{},
    Cache:  cache.NewMemory[Result](10*time.Minute, time.Minute),
    TTL:    5 * time.Minute,
}
```

### Use Rate Limiting

Prevent overwhelming downstream systems:

```go
Where: &condition.RateLimit[Event]{
    Limit: rate.Every(time.Second),
    Burst: 100,
}
```

## Error Handling

### Add Panic Recovery

Always include panic recovery middleware:

```go
Middlewares: []fh.Middleware[I, O]{
    &middlewares.Panic[I, O]{}, // First - catches all panics
    &middlewares.Slog[I, O]{},
}
```

### Log Errors

Sources should log errors:

```go
cb(ctx, event, func(err error) {
    if err != nil {
        log.Printf("Rule failed: %v", err)
    }
})
```

### Use Typed Errors

Create typed errors for better error handling:

```go
var ErrInvalidOrder = errors.New("invalid order")

func (a ProcessOrder) Process(ctx context.Context, order Order, _ boolexpr.Symbols) (Result, error) {
    if order.Amount <= 0 {
        return Result{}, ErrInvalidOrder
    }
    // ...
}
```

## Testing

### Test Components Independently

Unit test each component before integration:

```go
func TestProcessOrder(t *testing.T) {
    action := ProcessOrder{}
    event := OrderEvent{Amount: 100}

    result, err := action.Process(context.Background(), event, nil)

    assert.NoError(t, err)
    assert.Equal(t, 100.0, result.Total)
}
```

### Use Table-Driven Tests

Test multiple scenarios efficiently:

```go
func TestValidation(t *testing.T) {
    tests := []struct {
        name    string
        event   Event
        wantErr bool
    }{
        {"valid", Event{ID: "123"}, false},
        {"missing id", Event{}, true},
        {"negative amount", Event{ID: "123", Amount: -1}, true},
    }

    for _, tc := range tests {
        t.Run(tc.name, func(t *testing.T) {
            // Test logic
        })
    }
}
```

## Observability

### Add Structured Logging

Use `middlewares.Slog` for structured logs:

```go
Middlewares: []fh.Middleware[I, O]{
    &middlewares.Slog[I, O]{},
}
```

### Monitor Errors

Track errors and failures:

```go
cb(ctx, event, func(err error) {
    if err != nil {
        metrics.ErrorCount.Inc()
        log.Error("Processing failed", "error", err)
    }
})
```

## Configuration

### Validate Configuration

Check configuration at startup:

```go
type Config struct {
    KafkaBrokers string
    DatabaseURL  string
}

func (c Config) Validate() error {
    if c.KafkaBrokers == "" {
        return errors.New("KAFKA_BROKERS required")
    }
    return nil
}
```

### Use Environment-Specific Rules

Deploy different rules per environment:

```go
rule := &fh.SQLRule[Event, Output]{
    Environments: []string{"production"},
    // Production-specific behavior
}
```

## Security

### Use Context Timeouts

Prevent indefinite blocking:

```go
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

result, err := api.Call(ctx, request)
```

### Validate Permissions

Check authorization before processing:

```go
Where: condition.Func[Event](func(ctx context.Context, event Event, _ boolexpr.Symbols) (bool, error) {
    return auth.HasPermission(ctx, event.UserID, "resource.write"), nil
})
```

## Common Pitfalls

### Don't Share State Between Events

```go
// Bad - shared state
type StatefulAction struct {
    count int // Dangerous!
}

// Good - stateless
type StatelessAction struct{}
```

### Don't Block in Callbacks

```go
// Bad - blocks event processing
cb(ctx, event, func(err error) {
    time.Sleep(5 * time.Second) // Blocks!
})

// Good - async handling
cb(ctx, event, func(err error) {
    go handleAsync(err) // Non-blocking
})
```

### Don't Ignore Context

```go
// Bad - ignores context
func process(event Event) Result {
    return slowOperation(event)
}

// Good - respects context
func process(ctx context.Context, event Event) (Result, error) {
    return slowOperation(ctx, event)
}
```

## Checklist

Before deploying to production:

- [ ] All rules have descriptive IDs
- [ ] Panic recovery middleware enabled
- [ ] Logging middleware configured
- [ ] Error handling tested
- [ ] Context cancellation respected
- [ ] External dependencies mocked in tests
- [ ] Rate limiting configured
- [ ] Metrics/monitoring in place
- [ ] Environment-specific rules validated
- [ ] Documentation updated

## Next Steps

- Review [Examples](/examples/)
- Check [API Reference](/api/)
