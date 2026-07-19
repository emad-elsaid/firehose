# Testing

Learn how to test Firehose rules, components, and event processing pipelines effectively.

## Testing Philosophy

Firehose components are designed to be testable:

- **Sources** - Can be manually triggered
- **Actions** - Pure functions with no hidden state
- **Destinations** - Side effects can be captured
- **Conditions** - Boolean logic is deterministic
- **Rules** - Composable and isolatable

## Testing Sources

### Using Manual Source

The `sources.Manual` source allows you to emit events in tests:

```go
import "github.com/emad-elsaid/firehose/sources"

func TestEventProcessing(t *testing.T) {
    manual := &sources.Manual[OrderEvent]{}

    accumulator := &destinations.Accumulator[ProcessedOrder]{}

    rule := &fh.SQLRule[OrderEvent, ProcessedOrder]{
        ID:   "test_rule",
        Select: ProcessOrder{},
        Into:   accumulator,
        From:   manual,
    }

    ctx := context.Background()
    head, err := fh.Add(ctx, nil, rule)
    require.NoError(t, err)

    fh.Start(ctx, head, func(err error) {
        t.Errorf("Unexpected error: %v", err)
    })

    // Emit test events
    manual.Emit(ctx, OrderEvent{OrderID: "123", Amount: 100})
    manual.Emit(ctx, OrderEvent{OrderID: "456", Amount: 200})

    time.Sleep(50 * time.Millisecond) // Allow processing

    items := accumulator.Items()
    assert.Equal(t, 2, len(items))
}
```

### Testing Custom Sources

Test that your custom source emits events correctly:

```go
func TestCustomSource(t *testing.T) {
    var received []Event
    var mu sync.Mutex

    callback := func(ctx context.Context, event Event, rf fh.ErrorHandler) {
        mu.Lock()
        received = append(received, event)
        mu.Unlock()
    }

    source := MyCustomSource{}
    ctx, cancel := context.WithTimeout(context.Background(), time.Second)
    defer cancel()

    _, err := source.Start(ctx, callback)
    require.NoError(t, err)

    // Trigger source behavior
    // ...

    time.Sleep(100 * time.Millisecond)

    mu.Lock()
    defer mu.Unlock()
    assert.GreaterOrEqual(t, len(received), 1)
}
```

## Testing Actions

Actions are pure functions and easy to test:

```go
func TestProcessOrder(t *testing.T) {
    tests := []struct {
        name    string
        event   OrderEvent
        want    ProcessedOrder
        wantErr bool
    }{
        {
            name:  "valid order",
            event: OrderEvent{OrderID: "123", Amount: 100},
            want:  ProcessedOrder{ID: "123", Total: 100, Status: "processed"},
        },
        {
            name:    "invalid amount",
            event:   OrderEvent{OrderID: "123", Amount: -100},
            wantErr: true,
        },
    }

    for _, tc := range tests {
        t.Run(tc.name, func(t *testing.T) {
            action := ProcessOrder{}

            got, err := action.Process(context.Background(), tc.event, nil)

            if tc.wantErr {
                assert.Error(t, err)
            } else {
                assert.NoError(t, err)
                assert.Equal(t, tc.want, got)
            }
        })
    }
}
```

## Testing Destinations

### Using Accumulator

The `destinations.Accumulator` captures output for testing:

```go
func TestDestination(t *testing.T) {
    accumulator := &destinations.Accumulator[User]{}

    user := User{ID: "123", Name: "Alice"}
    err := accumulator.Send(context.Background(), user)

    assert.NoError(t, err)

    items := accumulator.Items()
    assert.Equal(t, 1, len(items))
    assert.Equal(t, user, items[0])
}
```

### Testing Custom Destinations

Test side effects with mocks or test doubles:

```go
func TestDatabaseDestination(t *testing.T) {
    db, mock, err := sqlmock.New()
    require.NoError(t, err)
    defer db.Close()

    mock.ExpectExec("INSERT INTO users").
        WithArgs("123", "Alice").
        WillReturnResult(sqlmock.NewResult(1, 1))

    dest := DatabaseDestination{DB: db}

    user := User{ID: "123", Name: "Alice"}
    err = dest.Send(context.Background(), user)

    assert.NoError(t, err)
    assert.NoError(t, mock.ExpectationsWereMet())
}
```

## Testing Conditions

### Table-Driven Tests

```go
func TestBusinessHoursCondition(t *testing.T) {
    tests := []struct {
        name string
        hour int
        want bool
    }{
        {"before hours", 8, false},
        {"start of hours", 9, true},
        {"during hours", 12, true},
        {"end of hours", 16, true},
        {"after hours", 17, false},
    }

    for _, tc := range tests {
        t.Run(tc.name, func(t *testing.T) {
            condition := BusinessHours{Start: 9, End: 17}

            // Mock time for testing
            event := Event{}

            got, err := condition.Evaluate(context.Background(), event, nil)

            require.NoError(t, err)
            assert.Equal(t, tc.want, got)
        })
    }
}
```

## Testing Complete Rules

### End-to-End Testing

```go
func TestCompleteRule(t *testing.T) {
    manual := &sources.Manual[OrderEvent]{}
    accumulator := &destinations.Accumulator[Email]{}

    rule := &fh.SQLRule[OrderEvent, Email]{
        ID:   "order_notification",
        Select: CreateEmail{},
        Into:   accumulator,
        Where:   condition.Cond[OrderEvent]("amount > 100"),
        From:   manual,
    }

    ctx := context.Background()
    head, err := fh.Add(ctx, nil, rule)
    require.NoError(t, err)

    fh.Start(ctx, head, nil)

    // Test event that passes condition
    manual.Emit(ctx, OrderEvent{OrderID: "123", Amount: 150})
    time.Sleep(50 * time.Millisecond)

    items := accumulator.Items()
    assert.Equal(t, 1, len(items))

    // Test event that fails condition
    manual.Emit(ctx, OrderEvent{OrderID: "456", Amount: 50})
    time.Sleep(50 * time.Millisecond)

    items = accumulator.Items()
    assert.Equal(t, 1, len(items)) // Still only 1
}
```

## Testing Middlewares

```go
func TestLoggingMiddleware(t *testing.T) {
    var logs []string
    var mu sync.Mutex

    logger := &TestLogger{
        LogFunc: func(msg string) {
            mu.Lock()
            logs = append(logs, msg)
            mu.Unlock()
        },
    }

    middleware := &LoggingMiddleware[Event, Output]{Logger: logger}

    action := &TestAction{}
    wrapped, err := middleware.WrapAction(context.Background(), &fh.SQLRule[Event, Output]{ID: "test"}, action)
    require.NoError(t, err)

    _, err = wrapped.Process(context.Background(), Event{}, nil)

    require.NoError(t, err)

    mu.Lock()
    defer mu.Unlock()
    assert.GreaterOrEqual(t, len(logs), 1)
}
```

## Best Practices

1. **Use table-driven tests** - Test multiple cases easily
2. **Test edge cases** - Nil values, empty data, errors
3. **Use Manual source for tests** - Deterministic event emission
4. **Use Accumulator for assertions** - Capture outputs easily
5. **Test components in isolation** - Unit test before integration
6. **Mock external dependencies** - Use interfaces and test doubles
7. **Test error conditions** - Verify error handling
8. **Use contexts with timeouts** - Prevent hanging tests
9. **Clean up resources** - Use defer and t.Cleanup()
10. **Skip slow tests in short mode** - `if testing.Short()`

## Next Steps

- Learn about [Environment Rules](/guide/environments)
- Explore [Best Practices](/guide/best-practices)
- See [Examples](/examples/)
